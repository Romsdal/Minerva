using System;
using System.Collections.Generic;
using Minerva.DB_Server.ConflictResolver;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;
using System.Threading;
using System.Threading.Tasks;
using Minerva.DB_Server.QueryExecutor;
using System.Diagnostics;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.Interface;
using Minerva.DB_Server.Network;
using System.Linq;
using Minerva.DB_Server.Raft;
using Minerva.DB_Server.Network.Protos;
using System.Threading.Channels;
using DotNext.Net.Cluster.Consensus.Raft;

namespace Minerva.DB_Server.MinervaLog;


public delegate void TxCompleteHandler(int tid, bool executed = false, string result = null);

/// <summary>
/// This is the class that handles coordination of the global log of batches.
/// </summary>
public class GlobalLogManager
{

    public const bool ENABLE_OCC = true;

    public int ReplicaId { get; init; }
    public bool IsCoordinator { get; set; }

    //public DictionaryStorage CurrentTemporaryState { get; set; }

    // index is replica id, and the list is the log of received batches from that replica
    // local replica's batch is not maintained in ths list
    public List<Batch>[] GlobalLogs { get; init; }
    public int CurrentEpochId { get; private set; } = 0;

    public TxCompleteHandler TxCompleteHandler { set; get; }

    // index is batchid, hashset is for unique replica ids that have acknowledged the batch
    public ConcurrentDictionary<int, ConcurrentBag<int>> ReceivedAcknowledgementCount { get; init; } = [];
    private int _majority;

    private int[] LastCommittedIndices;
    private int[] LastRequestConsistentCut;
    private int[] LastPoAIndices;
    private int[] ClusterEpoch;
    private readonly MinervaConfig _config;
    private readonly PersistentStorage _persistedDB;
    private readonly DeterministicExecutor _deterministicExecutor;
    private readonly MinervaTxOCCExecutor _occExecutor;
    private readonly ILogConnections _connection;
    public readonly RaftManager RaftNode;
    private readonly ConflictGraphSolver _solver;

    public int CurrentBatchId { get; private set; }
    public Batch CurrentBatch { get; private set; }

    private bool _configured = false;

    public bool HiContentionMode = !ENABLE_OCC;
    private int _hiContentionModeRemaining;


    private ILogger _logger = LoggerManager.GetLogger();

    public GlobalLogManager(MinervaConfig config, Cluster cluster, PersistentStorage persistedDB, MinervaTxOCCExecutor occExecutor, DeterministicExecutor deterministicExecutor, ILogConnections logConnection, RaftManager raftManager)
    {
        _config = config;
        _persistedDB = persistedDB;
        _occExecutor = occExecutor;
        _deterministicExecutor = deterministicExecutor;
        _connection = logConnection;
        _solver = new ConflictGraphSolver(_persistedDB);
        RaftNode = raftManager;
        RaftNode.OnLeaderChanged += OnLeaderChanged;

        ReplicaId = cluster.SelfNode.Id;
        int numNodes = cluster.Nodes.Length;
        GlobalLogs = new List<Batch>[numNodes];
        LastCommittedIndices = new int[numNodes];
        LastRequestConsistentCut = new int[numNodes];
        LastPoAIndices = new int[numNodes];
        ClusterEpoch = new int[numNodes];
        for (int i = 0; i < numNodes; i++)
        {

            GlobalLogs[i] = [];
            LastCommittedIndices[i] = -1;
            LastRequestConsistentCut[i] = -1;
            LastPoAIndices[i] = -1;
            ClusterEpoch[i] = -1;
        }

        _majority = (int)Math.Ceiling(numNodes / 2.0f);
        _hiContentionModeRemaining = 200 / _config.LocalEpochInterval; // stay in high contention mode for at least 200ms

        _occExecutor.SetNewTempStates([]);
    }

    public void OnLeaderChanged(RaftClusterMember leader)
    {
        // Check if this node is the new leader by comparing with local member address 
        bool isLeader = RaftNode.IsCurrentNodeLeader;
        
        if (isLeader)
        {
            _logger.LogInformation("This node is elected as the new leader");
            IsCoordinator = true;
        }
        else
        {
            IsCoordinator = false;
        }
    }
    

    public void Configure(bool isCoordinator)
    {
        IsCoordinator = isCoordinator;

        CurrentBatchId = 0;
        CurrentBatch = new Batch(0, ReplicaId);

        _configured = true;
        _logger.LogInformation("OCC Mode is set to {OCCMode}", ENABLE_OCC ? "ENABLED" : "DISABLED");
    }

    private DateTime _lastGlobalCommitTime = DateTime.MinValue;
    private DateTime _lastLocalCommitTime = DateTime.MinValue;

    private volatile bool _running = true;
    private int _lastCommittedEpochId = -1;

    public void RunTicker()
    {
        var t1 = new Thread(LocalEpochThread)
        {
            Name = "LocalEpochThread"
        };
        var t2 = new Thread(GlobalEpochThread)
        {
            Name = "GlobalEpochThread"
        };
        var t3 = new Thread(CommitHandlerThread)
        {
            Name = "CommitHandlerThread"
        };
        var t4 = new Thread(GarbageCollectThread)
        {
            Name = "GarbageCollectThread"
        };

        t1.Priority = ThreadPriority.AboveNormal;
        t2.Priority = ThreadPriority.Highest;
        t3.Priority = ThreadPriority.Highest;


        t1.Start();
        t2.Start();
        t3.Start();
        t4.Start();
    }

    private void GarbageCollectThread()
    {
        while (_running)
        {
            Thread.Sleep(500);
            GarbageCollect();
        }
    }


    private void LocalEpochThread()
    {
        while (_running)
        {
            Thread.Sleep(1);


            if (((DateTime.Now - _lastLocalCommitTime).TotalMilliseconds > _config.LocalEpochInterval || CurrentBatch.Size > _config.MaxBatchSize) && (CurrentBatch.Transactions.Count > 0 || CurrentBatchId == 0))
            {
                // Complete the current batch
                LocalBatchComplete();
                _lastLocalCommitTime = DateTime.Now;
            }

        }

    }

    private void GlobalEpochThread()
    {
        while (_running)
        {
            Thread.Sleep(1);

            if (IsCoordinator &&
            (DateTime.Now - _lastGlobalCommitTime).TotalMilliseconds > _config.CoordinatorGlobalEpochInterval)
            {

                bool flag = false;
                int[] tmpPoAIndices = new int[LastPoAIndices.Length];

                for (int i = 0; i < LastPoAIndices.Length; i++)
                {
                    if (LastPoAIndices[i] > LastRequestConsistentCut[i])
                    {
                        flag = true;
                        Array.Copy(LastPoAIndices, tmpPoAIndices, LastPoAIndices.Length);
                        break;
                    }
                }


                if (flag)
                {

                    ////_logger.LogTrace("[GlobalEpochThread] Initiating global commit at epoch {EpochId} with indices {LastPoAIndices}", CurrentEpochId, string.Join(", ", LastPoAIndices));
                    // Initiate global commit
                    RaftNode.LeaderReplicate([.. tmpPoAIndices], CurrentEpochId).Wait();      
                    _connection.SendCommittedGlobalCommitIdx(tmpPoAIndices, CurrentEpochId);
                    //RaftNode.AppendLog([.. tmpPoAIndices], CurrentEpochId);
                    _lastGlobalCommitTime = DateTime.Now;
                    CurrentEpochId++;
                    Array.Copy(tmpPoAIndices, LastRequestConsistentCut, LastPoAIndices.Length);
                }

            }
        }
    }

    private void CommitHandlerThread()
    {
        while (_running)
        {
            Thread.Sleep(1);
            try
            {
                int commitTo = -1;
                RaftNode.RaftLog.LogLock.EnterReadLock();
                commitTo = RaftNode.RaftLog.Log.Count;
                RaftNode.RaftLog.LogLock.ExitReadLock();

                if (commitTo > _lastCommittedEpochId + 1)
                {

                    for (int i = _lastCommittedEpochId + 1; i < commitTo; i++)
                    {
                        HandleRaftCommit([.. RaftNode.RaftLog.Log[i]], i);
                        _lastCommittedEpochId = i;

                    }

                    ClusterEpoch[ReplicaId] = _lastCommittedEpochId;
                    _connection.BroadcastAtEpoch(ReplicaId, _lastCommittedEpochId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[CommitHandlerThread] Exception in CommitHandlerThread: {Message} {StackTrace}", ex.Message, ex.StackTrace);
                throw;
            }
        }
    }

    public void Stop()
    {
        _running = false;
    }

    public void HandleRaftCommit(int[] ConsistentCutIndices, int epochId)
    {
        // formatted as "1,2,3:4" where 1,2,3 are the indices and 4 is the epochId
        if (!_configured)
        {
            throw new InvalidOperationException("GlobalLogManager is not initialized.");
        }

        try
        {
            GlobalCommit(ConsistentCutIndices, epochId);
            Array.Copy(ConsistentCutIndices, LastCommittedIndices, ConsistentCutIndices.Length);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[HandleRaftCommit] Exception during global commit: {Message} {StackTrace}", ex.Message, ex.StackTrace);
            throw;
        }
    }


    private Channel<WriteSetStore> _applyPDBChannel;
    public void GlobalCommit(int[] ConsistentCutIndices, int epochId)
    {
        Stopwatch sw = Stopwatch.StartNew();
        //_logger.LogTrace("[GlobalCommit] Starting global commit {EpochId} with {ConsistentCutIndices} : {LastCommittedIndices}", epochId, string.Join(", ", ConsistentCutIndices), string.Join(", ", LastCommittedIndices));

        RequestMissingBatches(ConsistentCutIndices);

        var batchesToCommit = CollectBatches(ConsistentCutIndices);

        if (batchesToCommit.Length == 0)
        {
            ////_logger.LogTrace("[GlobalCommit] No batches to commit in epoch {EpochId}", epochId);
            return;
        }


        Stopwatch sw2 = Stopwatch.StartNew();
        int goodTxCount = _solver.FindConflicts(batchesToCommit, GlobalLogs.Length, _config.SolverExact);
        _solver.Reset();
        sw2.Stop();
        //_logger.LogTrace("[GlobalCommit] Conflict detection for epoch {EpochId} took {ElapsedMilliseconds} ms", epochId, sw2.Elapsed.TotalMilliseconds);


        sw2.Restart();
        // we want to sort by both replica id and transaction id
        List<TransactionRecord>[] toReExecute = new List<TransactionRecord>[GlobalLogs.Length];
        for (int i = 0; i < GlobalLogs.Length; i++)
        {
            toReExecute[i] = [];
        }
        List<TransactionRecord>[] orderedStales = new List<TransactionRecord>[GlobalLogs.Length];
        for (int i = 0; i < GlobalLogs.Length; i++)
        {
            orderedStales[i] = [];
        }
        List<TransactionRecord>[] nonExecuted = new List<TransactionRecord>[GlobalLogs.Length];
        for (int i = 0; i < GlobalLogs.Length; i++)
        {
            nonExecuted[i] = [];
        }

        int txCountRaw = 0;
        int txCount = 0;

        bool useBackgroundThread = goodTxCount > 200;
        Task applyTask = null;
        if (useBackgroundThread)
        {
            _applyPDBChannel = Channel.CreateUnbounded<WriteSetStore>();
            // Start a background thread to apply WriteSets to the persistent DB
            applyTask = Task.Run(() => ApplyPersitDBThread(epochId));
        }

        foreach (var r in batchesToCommit)
        {
            foreach (var batch in r)
            {
                foreach (var tx in batch.Transactions)
                {
                    txCountRaw++;

                    if (tx.ConflictStatus == TxConflict.None)
                    {

                        if (useBackgroundThread)
                        {
                            // write non-conflicting transactions to the database
                            // Send WriteSet to background thread via channel
                            _applyPDBChannel.Writer.TryWrite(tx.WriteSet);
                        }
                        else
                        {
                            TxStateHelpers.ApplyWriteSetToPersistentDB(tx.WriteSet, _persistedDB, epochId);
                        }

                        // only notify the transaction completion if it is my batch
                        if (batch.SourceReplicaId == ReplicaId && TxCompleteHandler is not null)
                        {
                            // why is this thing so slow???
                            _ = Task.Run(() => TxCompleteHandler(tx.Tid));
                        }
                        txCount += 1;
                    }
                    else if (tx.ConflictStatus == TxConflict.Conflict)
                    {
                        toReExecute[batch.SourceReplicaId].Add(tx);
                        _persistedDB.ReExOriginalTransactions.Add((batch.SourceReplicaId, tx.Tid));
                    }
                    else if (tx.ConflictStatus == TxConflict.Stale)
                    {
                        orderedStales[batch.SourceReplicaId].Add(tx);
                        _persistedDB.ReExOriginalTransactions.Add((batch.SourceReplicaId, tx.Tid));
                    }
                    else if (tx.ConflictStatus == TxConflict.NonExecuted)
                    {
                        nonExecuted[batch.SourceReplicaId].Add(tx);
                        _persistedDB.ReExOriginalTransactions.Add((batch.SourceReplicaId, tx.Tid));
                    }
                    
                }
                batch.Status = BatchStatus.Committed;
            }
        }

        // use insertion sort because transactions are likely mostly sorted
        Sort.SortTransactionsByTID(toReExecute);
        Sort.SortTransactionsByTID(orderedStales);
        Sort.SortTransactionsByTID(nonExecuted);

        int stalesCount = orderedStales.Sum(q => q.Count);
        int ConflictedTx = toReExecute.Sum(q => q.Count);
        int NonLocalExecutedTx = nonExecuted.Sum(q => q.Count);

        // Signal completion and wait for background thread to finish
        if (useBackgroundThread)
        {
            _applyPDBChannel.Writer.Complete();
            applyTask.Wait();
        }


        sw2.Stop();
        //_logger.LogTrace("[GlobalCommit] Applying non-conflicting transactions for epoch {EpochId} took {ElapsedMilliseconds} ms", epochId, sw2.Elapsed.TotalMilliseconds);

        sw2.Restart();

        _deterministicExecutor.DeterministicExecutionAsync(orderedStales, _config.ReplicaPriority, TxCompleteHandler, epochId).Wait();
        _deterministicExecutor.DeterministicExecutionAsync(toReExecute, _config.ReplicaPriority, TxCompleteHandler, epochId).Wait();
        _deterministicExecutor.DeterministicExecutionAsync(nonExecuted, _config.ReplicaPriority, TxCompleteHandler, epochId).Wait();
        sw2.Stop();
        //_logger.LogTrace("[GlobalCommit] Re-executing took {ElapsedMilliseconds} ms", sw2.Elapsed.TotalMilliseconds);

        if (!IsCoordinator)
        {
            CurrentEpochId = epochId;
        }
        _occExecutor.SetNewTempStates();

        _lastGlobalCommitTime = DateTime.Now;


        Stats.TotalAppliedTx += txCountRaw;
        Stats.StaledTxs += stalesCount;
        Stats.ConflictedTx += ConflictedTx;
        Stats.NonLocalExecutedTx += NonLocalExecutedTx;

        //_logger.LogTrace("[GlobalCommit] Epoch {EpochId} committed with {txCountRaw} transactions (stales: {stalesCount}, conflict: {reExecuteCount}, non-local: {NonLocalExecutedTx})", epochId, txCountRaw, stalesCount, ConflictedTx, NonLocalExecutedTx);
        ////_logger.LogTrace("[GlobalCommit] Distance to PoA: {DistanceToPoA} sum {distance sum}", string.Join(", ", LastPoAIndices.Select((poa, i) => poa - LastCommittedIndices[i])), LastPoAIndices.Zip(LastCommittedIndices, (poa, last) => poa - last).Sum());
        sw.Stop();
        //_logger.LogTrace("[GlobalCommit] Epoch {EpochId} commit took {ElapsedMilliseconds} ms", epochId, sw.Elapsed.TotalMilliseconds);
        
        if (txCount < stalesCount + ConflictedTx)
        {
            if (!Interlocked.CompareExchange(ref HiContentionMode, true, false))
            {
                Interlocked.Exchange(ref _hiContentionModeRemaining, 5);
                _logger.LogWarning("[GlobalCommit] Entering high contention mode");
            }            
        }

    }

    private async Task ApplyPersitDBThread(int epochId)
    {
        await foreach (var writeSet in _applyPDBChannel.Reader.ReadAllAsync())
        {
            TxStateHelpers.ApplyWriteSetToPersistentDB(writeSet, _persistedDB, epochId);
        }
    }



    private void RequestMissingBatches(int[] ConsistentCutIndices)
    {
        int repeat = 1;
        while (true)
        {
            HashSet<(int, int)> _missingBatches = [];
            // Request batches that are not available in the global log

            for (int i = 0; i < GlobalLogs.Length; i++)
            {

                if (ConsistentCutIndices[i] < LastCommittedIndices[i])
                {
                    _logger.LogError("{log}", string.Join(',', RaftNode.RaftLog.Log));
                }

                Debug.Assert(ConsistentCutIndices[i] >= LastCommittedIndices[i], $"Consistent cut index {ConsistentCutIndices[i]} should be greater than or equal to last committed index {LastCommittedIndices[i]} for replica {i}");

                LastPoAIndices[i] = Math.Max(LastPoAIndices[i], LastCommittedIndices[i]);

                for (int j = LastCommittedIndices[i] + 1; j <= ConsistentCutIndices[i]; j++)
                {
                    if (GlobalLogs[i].Count <= j || GlobalLogs[i][j] is null)
                    {
                        _missingBatches.Add((i, j));

                        while (GlobalLogs[i].Count <= j)
                        {
                            GlobalLogs[i].Add(null); // Ensure the log has enough space
                        }
                    }
                }
            }

            if (_missingBatches.Count > 0)
            {
                ////_logger.LogTrace("[RequestMissingBatches] Requesting {MissingBatchCount} missing batches for global commit", _missingBatches.Count);
                //_connection.RequestBatches([.. _missingBatches]);
            }
            else
            {
                return;
            }
            Thread.Sleep(Math.Min(repeat * 2, 20)); // wait for batches to arrive
            repeat++;
        }
    }

    /// <summary>
    /// Collects all the batches from the last committed indices to the cut indices, and 
    /// merge the chains from the same replica into a single chain
    /// </summary>
    /// <param name="ConsistentCutIndices"></param>
    private List<Batch>[] CollectBatches(int[] ConsistentCutIndices)
    {
        List<Batch>[] batchesToCommit = new List<Batch>[GlobalLogs.Length];
        for (int i = 0; i < GlobalLogs.Length; i++)
        {
            batchesToCommit[i] = [];
        }

        for (int i = 0; i < GlobalLogs.Length; i++)
        {
            if (ConsistentCutIndices[i] > LastCommittedIndices[i])
            {
                for (int j = LastCommittedIndices[i] + 1; j <= ConsistentCutIndices[i]; j++)
                {
                    Debug.Assert(GlobalLogs[i][j] is not null, $"Batch {i}:{j} is null during collection for global commit.");
                    batchesToCommit[i].Add(GlobalLogs[i][j]);
                }
            }
        }

        return batchesToCommit;
    }


    // ===== Methods To Handle Log Replication ======


    public void ReceivedBatch(Batch batch, bool doAck = true)
    {

        ////_logger.LogTrace("[ReceivedBatch] Received batch {ReplicaId}:{BatchId} ", batch.SourceReplicaId, batch.BatchId);

        lock (GlobalLogs[batch.SourceReplicaId])
        {
            while (GlobalLogs[batch.SourceReplicaId].Count <= batch.BatchId)
            {
                GlobalLogs[batch.SourceReplicaId].Add(null);
            }

            var temp = GlobalLogs[batch.SourceReplicaId][batch.BatchId];
            if (temp is not null)
            {
                // Batch already exists, ignore the new one
                return;
            }

            GlobalLogs[batch.SourceReplicaId][batch.BatchId] = batch;
        }

        // If I received the PoA before I received the batch
        if (LastPoAIndices[batch.SourceReplicaId] >= batch.BatchId)
        {
            batch.Status = BatchStatus.PoASent;
        }

        if (doAck)
        {
            ////_logger.LogTrace("[ReceivedBatch] Acknowledging batch {ReplicaId}:{BatchId} ", batch.SourceReplicaId, batch.BatchId);
            _connection.AcknowledgeBatch(batch.SourceReplicaId, batch.BatchId);
        }

    }

    public void ReceivedPoA(int sourceReplicaId, int batchId)
    {
        // because we know that if we received a PoA, then all previous batches are also available
        LastPoAIndices[sourceReplicaId] = Math.Max(LastPoAIndices[sourceReplicaId], batchId);
        ////_logger.LogTrace("[ReceivedPoA] Received PoA for batch {ReplicaId}:{BatchId} , current PoA index: {PoAIndex}", sourceReplicaId, batchId, LastPoAIndices[sourceReplicaId]);

        lock (GlobalLogs[sourceReplicaId])
        {
            // add global log entry until the last available index
            while (GlobalLogs[sourceReplicaId].Count <= batchId)
            {
                GlobalLogs[sourceReplicaId].Add(null);
            }
        }


        // request for batches until the PoA index
        for (int i = LastCommittedIndices[sourceReplicaId] + 1; i <= batchId; i++)
        {
            if (GlobalLogs[sourceReplicaId][i] is null)
            {
                //_connection.RequestBatch(sourceReplicaId, i);
            }
            else
            {
                GlobalLogs[sourceReplicaId][i].Status = BatchStatus.PoASent;
            }
        }

    }

    public void ReceivedBatchRequest(int requestReplicaId, int sourceReplicaId, int batchId)
    {
        Batch batch = null;
        if (GlobalLogs[sourceReplicaId].Count > batchId)
        {
            batch = GlobalLogs[sourceReplicaId][batchId];
        }

        if (batch is not null)
        {
            _connection.SendBatch(requestReplicaId, batch);
            ////_logger.LogTrace("[ReceivedBatchRequest] Sent batch {sourceReplicaId}:{batchId} to replica {requestReplicaId}", sourceReplicaId, batchId, requestReplicaId);
        }
    }


    /// <summary>
    /// Handler for receiving acknowledgements from other replicas for
    /// batches that I just broadcasted.
    /// </summary>
    /// <param name="fromReplica"></param>
    /// <param name="batchId"></param>        

    private readonly object AckLock = new();
    public void ReceivedAcknowledgement(int formReplica, int sourceReplicaId, int batchId)
    {
        // if it's not my batch, then ignore it
        if (sourceReplicaId != ReplicaId)
        {
            return;
        }

        lock (AckLock)
        {
            var batch = GlobalLogs[ReplicaId][batchId];
            if (!ReceivedAcknowledgementCount.TryGetValue(batchId, out var ackSet))
            {
                ackSet = [];
                ReceivedAcknowledgementCount[batchId] = ackSet;
            }
            ackSet.Add(formReplica);


            ////_logger.LogTrace("[ReceivedAcknowledgement] Received acknowledgement for batch {ReplicaId}:{BatchId} from {source}", sourceReplicaId, batchId, formReplica);


            // do nothing 
            if (batch.Status != BatchStatus.LocalCompleted)
            {
                ////_logger.LogTrace("[ReceivedAcknowledgement] Batch {ReplicaId}:{BatchId} is not in LocalCompleted status, current status: {Status}", ReplicaId, batchId, batch.Status);
                return;
            }

            int uniqueAckCount = ackSet.Distinct().Count();

            ////_logger.LogTrace("[ReceivedAcknowledgement] Batch {ReplicaId}:{BatchId} has {UniqueAckCount} unique acknowledgements", ReplicaId, batchId, uniqueAckCount);

            if (uniqueAckCount >= _majority)
            //if (uniqueAckCount >= GlobalLogs.Length) // TODO:
            {
                ////_logger.LogTrace("[ReceivedAcknowledgement] Batch {ReplicaId}:{BatchId} has received majority acknowledgements, marking it as Available", ReplicaId, batchId);
                batch.Status = BatchStatus.Available;
                ReceivedAcknowledgementCount.TryRemove(batchId, out _);
            }
            else
            {
                return;
            }

            // If all it's previous batches are available, then send PoA
            for (int i = LastPoAIndices[ReplicaId] + 1; i < GlobalLogs[ReplicaId].Count; i++)
            {
                if (GlobalLogs[ReplicaId][i].Status != BatchStatus.Available)
                {
                    ////_logger.LogTrace("[ReceivedAcknowledgement] Batch {ReplicaId}:{BatchId} is not Available yet, current status: {Status}", ReplicaId, i, GlobalLogs[ReplicaId][i].Status);
                    return;
                }

                _connection.BroadcastPoA(ReplicaId, i);
                LastPoAIndices[ReplicaId] = i;
                GlobalLogs[ReplicaId][i].Status = BatchStatus.PoASent;
                ////_logger.LogTrace("[ReceivedAcknowledgement] Sent PoA for batch {ReplicaId}:{BatchId}", ReplicaId, batchId);
            }
        }



    }

    //======================================== Test Only Method ======================== 
    [Obsolete("This method is for testing purposes only.")]
    public void ManualGlobalCommit(int[] ConsistentCutIndices, int epochId)
    {
        GlobalCommit(ConsistentCutIndices, epochId);
    }

    [Obsolete("This method is for testing purposes only.")]
    public void ManualInitGlobalCommit()
    {
        _connection.RequestGlobalCommit(LastPoAIndices, CurrentEpochId);
        CurrentEpochId++;
    }


    //======================================== Local Log Methods ========================
    private readonly Lock CurrentBatchLock = new();
    public void TxComplete(ClientRequest Query, MinervaTx tx)
    {
        lock (CurrentBatchLock)
        {
            CurrentBatch.Add(Query, tx);
            //////_logger.LogTrace("[TxComplete] Added transaction {TransactionId} to current batch {ReplicaId}:{BatchId}", tx.Tid, ReplicaId, CurrentBatchId);
        }

    }

    /// <summary>
    /// Filling the current batch
    /// </summary>
    private void LocalBatchComplete()
    {
        int bid;
        int txCount;
        lock (CurrentBatchLock)
        {
            bid = CurrentBatchId;
            txCount = CurrentBatch.Transactions.Count;
            CurrentBatch.Status = BatchStatus.LocalCompleted;
            GlobalLogs[ReplicaId].Add(CurrentBatch);

            _connection.BroadcastBatch(CurrentBatch);

            CurrentBatchId++;
            CurrentBatch = new Batch(CurrentBatchId, ReplicaId);
        }

        if (HiContentionMode && ENABLE_OCC)
        {
            var val = Interlocked.Decrement(ref _hiContentionModeRemaining);
            if (val <= 0)
            {
                _logger.LogWarning("[LocalBatchComplete] Exiting high contention mode");
                Interlocked.Exchange(ref HiContentionMode, false);
            }
        }


        ////_logger.LogTrace("[LocalBatchComplete] Broadcasted batch {ReplicaId}:{BatchId} with {TransactionCount} transactions", ReplicaId, bid, txCount);
        // immediately acknowledge the batch as self 
        ReceivedAcknowledgement(ReplicaId, ReplicaId, bid);
    }

    //======================================== Test Only Method ======================== 
    [Obsolete("This method is for testing purposes only.")]
    public void ManualBatchComplete()
    {
        LocalBatchComplete();
    }


    //======================================== Garbage Collect ========================

    public void SetClusterEpochInfo(int replicaId, int epochID)
    {

        ClusterEpoch[replicaId] = Math.Max(ClusterEpoch[replicaId], epochID);
        ////_logger.LogTrace("[SetClusterEpochInfo] Received cluster epoch info from replica {ReplicaId}: epoch {EpochId}. Current cluster epochs: {ClusterEpochs}", replicaId, epochID, string.Join(", ", ClusterEpoch));
    }

    int _lastGarbageCollectEpoch = 0;
    public void GarbageCollect()
    {
        int MostBehindEpoch = ClusterEpoch.Min();
        if (MostBehindEpoch > _lastGarbageCollectEpoch && MostBehindEpoch > 0)
        {
            var lastGCIndices = RaftNode.RaftLog.Log[_lastGarbageCollectEpoch];
            var commitToIndices = RaftNode.RaftLog.Log[MostBehindEpoch];
            // set all log from _lastGarbageCollectEpoch to MostBehindEpoch to null
            for (int i = 0; i < GlobalLogs.Length; i++)
            {
                for (int j = lastGCIndices[i] + 1; j <= commitToIndices[i]; j++)
                {
                    if (GlobalLogs[i][j] is not null)
                    {
                        GlobalLogs[i][j] = null;
                    }
                }
            }
            //GC.Collect();
            _lastGarbageCollectEpoch = MostBehindEpoch;
            ////_logger.LogTrace("[GarbageCollect] Garbage collected logs from epoch {StartEpoch} to {EndEpoch}", _lastGarbageCollectEpoch + 1, MostBehindEpoch);
        }
    }

}



