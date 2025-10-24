using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;
using static Minerva.DB_Server.Storage.PersistentStorage;

namespace Minerva.DB_Server.ConflictResolver;

public class TransactionsChain(int sourceReplicaId)
{
    public int SolverIndex { get; set; }
    public int SourceReplicaId { get; init; } = sourceReplicaId;
    public List<TransactionRecord> Records { get; init; } = [];
    public bool IsStale { get; set; }
}

public class ConflictGraphSolver(PersistentStorage persistedDB)
{

    private readonly PersistentStorage _persistedDB = persistedDB;
    private ConflictGraph _conflictGraph;
    private StableSolver _stableSolver;

    private IDictionary<int, TransactionsChain> _solverVertexIdtoTxc;
    private List<TransactionsChain> _txChains;

    public ILogger _logger = LoggerManager.GetLogger();

    /// <summary>
    /// Use a MWIS solver to find conflicts in the global epoch batches.
    /// </summary>
    /// <param name="globalEpochBatches">List of batches to commit, formatted as an array of [source replica id] and the list of batches</param>
    /// <param name="numNodes"></param>
    /// <param name="exact"></param>
    /// <returns></returns>
    public int FindConflicts(List<Batch>[] globalEpochBatches, int numNodes, bool exact)
    {
        int count = 0;
        _conflictGraph = new StableSolverGraph();

        ConstructConflictGraph(globalEpochBatches);

        if (_conflictGraph.isBuild)
        {
            using (_stableSolver = new StableSolver())
            {
                Stopwatch sw = Stopwatch.StartNew();
                var remaining = FilterConflicts(exact);

                foreach (var vertexId in remaining)
                {
                    var txc = _solverVertexIdtoTxc[vertexId];
                    foreach (var tx in txc.Records)
                    {
                        tx.ConflictStatus = TxConflict.None;
                        count++;
                    }
                }

                _conflictGraph.Dispose();
                sw.Stop();
                //_logger.LogTrace($"Conflict detection took {sw.Elapsed.TotalMilliseconds} ms");

                return count;
            }
        }
        else
        {
            throw new InvalidOperationException("Conflict graph is not built.");
        }
    }

    private void ConstructConflictGraph(List<Batch>[] globalEpochBatches)
    {

        Stopwatch sw = Stopwatch.StartNew();
        ConstructTxChains(globalEpochBatches);
        sw.Stop();
        //_logger.LogTrace($"Constructing transaction chains took {sw.Elapsed.TotalMilliseconds} ms");



        int vertexIndex = 0;

        void ProcessChain(TransactionsChain txc)
        {
            if (!CheckChainContainsStaleTransaction(txc))
            {

                foreach (var tx in txc.Records)
                {
                    tx.ConflictStatus = TxConflict.Stale;
                }
                txc.IsStale = true;

                return;
            }

            txc.IsStale = false;
            int currentIndex = Interlocked.Increment(ref vertexIndex) - 1;
            _solverVertexIdtoTxc[currentIndex] = txc;
            txc.SolverIndex = currentIndex;
        }

        sw.Restart();
        if (_txChains.Count < 100)
        {
            _solverVertexIdtoTxc = new Dictionary<int, TransactionsChain>(_txChains.Count);

            foreach (var txc in _txChains)
            {
                ProcessChain(txc);
            }
        }
        else
        {
            _solverVertexIdtoTxc = new ConcurrentDictionary<int, TransactionsChain>(4, _txChains.Count);

            var partitioner = Partitioner.Create(_txChains, EnumerablePartitionerOptions.NoBuffering);
            Parallel.ForEach(partitioner.GetPartitions(4), new ParallelOptions { MaxDegreeOfParallelism = 4 }, partition =>
            {
                using (partition)
                {
                    while (partition.MoveNext())
                    {
                        ProcessChain(partition.Current);
                    }
                }
            });
        }
        sw.Stop();
        //_logger.LogTrace($"Processing transaction chains took {sw.Elapsed.TotalMilliseconds} ms");

        _conflictGraph.AddVertices(vertexIndex);
        // Set weights for the vertices
        foreach (var txc in _solverVertexIdtoTxc.Values)
        {
            _conflictGraph.SetWeight(txc.SolverIndex, txc.Records.Count);
        }

        // construct conflict graphs
        // reminder: a transaction (chain) is identified by (replica_id, transaction(_chain_id))

        sw.Restart();
        using var rwTrackers = new ConflictGraphTrackers(_txChains.Count);
        foreach (var txc in _txChains)
        {
            if (txc.IsStale)
                continue;

            rwTrackers.AddRWSet(txc);
        }
        sw.Stop();
        //_logger.LogTrace($"Constructing read/write trackers took {sw.Elapsed.TotalMilliseconds} ms");

        sw.Restart();

        WWConflict(rwTrackers.YCSBWriteTracker);
        WWConflict(rwTrackers.WarehouseWriteTracker);
        WWConflict(rwTrackers.DistrictWriteTracker);
        WWConflict(rwTrackers.CustomerWriteTracker);
        WWConflict(rwTrackers.ItemWriteTracker);
        WWConflict(rwTrackers.StockWriteTracker);
        WWConflict(rwTrackers.HistoryWriteTracker);
        WWConflict(rwTrackers.NewOrderWriteTracker);
        WWConflict(rwTrackers.OrderWriteTracker);
        WWConflict(rwTrackers.OrderLineWriteTracker);

        RWConflict(rwTrackers.YCSBWriteTracker, rwTrackers.YCSBReadTrackers);
        RWConflict(rwTrackers.WarehouseWriteTracker, rwTrackers.WarehouseReadTrackers);
        RWConflict(rwTrackers.DistrictWriteTracker, rwTrackers.DistrictReadTrackers);
        RWConflict(rwTrackers.CustomerWriteTracker, rwTrackers.CustomerReadTrackers);
        RWConflict(rwTrackers.ItemWriteTracker, rwTrackers.ItemReadTrackers);
        RWConflict(rwTrackers.StockWriteTracker, rwTrackers.StockReadTrackers);
        RWConflict(rwTrackers.HistoryWriteTracker, rwTrackers.HistoryReadTrackers);
        RWConflict(rwTrackers.NewOrderWriteTracker, rwTrackers.NewOrderReadTrackers);
        RWConflict(rwTrackers.OrderWriteTracker, rwTrackers.OrderReadTrackers);
        RWConflict(rwTrackers.OrderLineWriteTracker, rwTrackers.OrderLineReadTrackers);

        sw.Stop();
        //_logger.LogTrace($"Building conflict graph took {sw.Elapsed.TotalMilliseconds} ms");

        sw.Restart();
        _conflictGraph.Build();
        sw.Stop();
        //_logger.LogTrace($"Finalizing conflict graph took {sw.Elapsed.TotalMilliseconds} ms");

    }

    private void WWConflict(UnsafeConflictTracker tracker)
    {
        if (tracker is null)
            return;

        foreach (var bucket in tracker)
        {
            var entries = bucket.Entries;
            if (entries.Length <= 1)
            {
                continue;
            }

            for (int i = 0; i < entries.Length; i++)
            {
                var txc1 = entries[i];
                for (int j = i + 1; j < entries.Length; j++)
                {
                    var txc2 = entries[j];
                    if (txc1.Rid != txc2.Rid)
                    {
                        _conflictGraph.AddEdge(txc1.TxcIdx, txc2.TxcIdx, true);
                    }
                }
            }
        }
    }

    private void RWConflict(UnsafeConflictTracker writeTracker, UnsafeConflictTracker readTracker)
    {
        if (readTracker is null || writeTracker is null)
            return;

        foreach (var readBucket in readTracker)
        {
            var readEntries = readBucket.Entries;
            if (readEntries.IsEmpty)
            {
                continue;
            }

            if (!writeTracker.TryGetBucket(readBucket.Key, out var writeBucket))
            {
                continue;
            }

            var writeEntries = writeBucket.Entries;
            if (writeEntries.IsEmpty)
            {
                continue;
            }

            for (int i = 0; i < readEntries.Length; i++)
            {
                var readTx = readEntries[i];
                for (int j = 0; j < writeEntries.Length; j++)
                {
                    var writeTx = writeEntries[j];
                    if (readTx.TxcIdx != writeTx.TxcIdx && readTx.Rid != writeTx.Rid)
                    {
                        _conflictGraph.AddEdge(readTx.TxcIdx, writeTx.TxcIdx, true);
                    }
                }
            }
        }
    }

    // combine dependent transactions of the same replica into a single chain
    // and use them as a single unit for conflict checking
    private void ConstructTxChains(List<Batch>[] globalEpochBatches)
    {
        _txChains = [];

    using var builder = new UnsafeReplicaChainBuilder();
        for (int replicaId = 0; replicaId < globalEpochBatches.Length; replicaId++)
        {
            var replicaBatches = globalEpochBatches[replicaId];
            builder.BuildChains(replicaId, replicaBatches, _txChains);
        }
    }

    // stale only possible with the following conditions:
    // 1. tx has no dependency  - means that is reading from a snapshot
    // 2. The key is not updated in the last epoch
    // or
    // 3. tx has dependency - but the dependency was re-executed, of course the dependency must from the previous epoch
    private bool CheckChainContainsStaleTransaction(TransactionsChain chain)
    {
        foreach (var tx in chain.Records)
        {
            // if Tx has no dependency, check if any read from snapshot is stale

            if (!CheckStalePerDB<(int, string), string>(tx.KeyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot, Database.YCSB) ||
                !CheckStalePerDB<long, Warehouse>(tx.KeyAccessedFromSnapshot.WarehouseReadsFromSnapshot, Database.Warehouse) ||
                !CheckStalePerDB<(long, long), District>(tx.KeyAccessedFromSnapshot.DistrictReadsFromSnapshot, Database.District) ||
                !CheckStalePerDB<(long, long, long), Customer>(tx.KeyAccessedFromSnapshot.CustomerReadsFromSnapshot, Database.Customer) ||
                !CheckStalePerDB<long, Item>(tx.KeyAccessedFromSnapshot.ItemReadsFromSnapshot, Database.Item) ||
                !CheckStalePerDB<(long, long), Stock>(tx.KeyAccessedFromSnapshot.StockReadsFromSnapshot, Database.Stock) ||
                !CheckStalePerDB<(long, long), History>(tx.KeyAccessedFromSnapshot.HistoryReadsFromSnapshot, Database.History) ||
                !CheckStalePerDB<(long, long, long), NewOrder>(tx.KeyAccessedFromSnapshot.NewOrderReadsFromSnapshot, Database.NewOrder) ||
                !CheckStalePerDB<(long, long, long), Order>(tx.KeyAccessedFromSnapshot.OrderReadsFromSnapshot, Database.Order) ||
                !CheckStalePerDB<(long, long, long, long), OrderLine>(tx.KeyAccessedFromSnapshot.OrderLineReadsFromSnapshot, Database.OrderLine))
            {
                return false;
            }

            // if Tx has dependency, check if all dependency transactions are in the committed original transactions set
            foreach (var prev in tx.PrevTids)
            {
                // if the dependency transaction is not in the committed original transactions set, it is stale
                if (_persistedDB.ReExOriginalTransactions.Contains((chain.SourceReplicaId, prev)))
                {
                    return false;
                }
            }

        }

        return true;
    }

    // return true if all keys are not stale, false otherwise
    private bool CheckStalePerDB<Tkey, TValue>(Dictionary<Tkey, int> keyAccessedFromSnapshot, Database database)
    {
        // check all the keys that were read from snapshot
        foreach (var (key, accessedFromCid) in keyAccessedFromSnapshot)
        {
            _persistedDB.Get(database, key, out var entry);
            var lastUpdatedCid = ((PersistEntry<TValue>)entry).Cid;

            // if the key is updated after it was accessed by the transaction, it is stale
            if (lastUpdatedCid > accessedFromCid)
            {
                return false;
            }
        }

        return true;
    }



    private int[] FilterConflicts(bool exact)
    {

        if (_conflictGraph.isBuild == false)
        {
            throw new InvalidOperationException("Conflict graph is not built.");
        }

        // greedy_gwmin2 seems to have the best result/performance tradeoff
        // exact solution stuck when over 4000 vertices... lets hope we don't have that many at a time
        int type = exact ? 5 : 3;
        (bool isFeasible, int[] vertices, _) = _stableSolver.Solve((StableSolverGraph)_conflictGraph, type);

        if (!isFeasible)
        {
            throw new InvalidOperationException("No feasible solution found for the conflict graph.");
        }
        return vertices;
    }

    public void Reset()
    {
        _conflictGraph = null;
        _stableSolver = null;
        _solverVertexIdtoTxc = null;
        //_solverTxctoVertexId = null;
        _txChains.Clear();
    }



    // =============== Test Only Methods ===============
    [Obsolete("This method is for testing purposes only.")]
    public void ConstructConflictGraphWithoutSolver(List<Batch>[] globalEpochBatches, ConflictGraph conflictGraph)
    {
        _conflictGraph = conflictGraph;
        ConstructConflictGraph(globalEpochBatches);
    }
}


