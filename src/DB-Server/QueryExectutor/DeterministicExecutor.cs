using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using DB_Server.QueryExectutor;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.QueryExecutor;

public class DeterministicExecutor(IQueryParser queryParser, PersistentStorage persistedDB, int replicaId)
{
    private readonly IQueryParser _queryParser = queryParser;
    private readonly int ReplicaId = replicaId;
    private DeterministicTxManager _transactionManager;
    private DeterminsticLockManager _lockManager = new();
    private readonly ILogger _logger = LoggerManager.GetLogger();



    public async Task DeterministicExecutionAsync(List<TransactionRecord>[] transactions, int[] replicaPriority, TxCompleteHandler txCompleteHandler, int curEpoch)
    {
        _transactionManager = new(persistedDB, curEpoch);

        int totalTransactions = transactions.Sum(q => q.Count);
        bool sequentialRun = totalTransactions <= 80;

        if (sequentialRun)
        {
            foreach (var sourceNodeId in replicaPriority)
            {
                foreach (var originalTx in transactions[sourceNodeId])
                {
                    var newTx = _queryParser.ParseSQL(originalTx.Query, _transactionManager).Execute();
                    if (sourceNodeId == ReplicaId)
                    {
                        txCompleteHandler(originalTx.Tid, true, newTx.Result);
                    }
                }
            }

            return;
        }
        else
        {
            foreach (var sourceNodeId in replicaPriority)
            {
                foreach (var originalTx in transactions[sourceNodeId])
                {
                    originalTx.SourceReplicaId = sourceNodeId;
                    _lockManager.Lock(originalTx);
                }
            }

            List<Task> tasks = new(totalTransactions);
            while (_lockManager.RreadyTxns.Count > 0 || _lockManager.RtxnWaits.Count > 0)
            {
                while (_lockManager.RreadyTxns.TryDequeue(out TransactionRecord tx))
                {
                    tasks.Add(ExecuteSingle(tx, txCompleteHandler));
                }
            }

            await Task.WhenAll([.. tasks]);
        }
    }

    public async Task ExecuteSingle(TransactionRecord tx, TxCompleteHandler txCompleteHandler)
    {

        Transaction newTx = null;

        await Task.Run(() =>
        {
            bool executed = false;
            string res = null;
            try
            {
                newTx = _queryParser.ParseSQL(tx.Query, _transactionManager).Execute();
                res = newTx.Result;
                executed = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing transaction {TransactionId}: {ErrorMessage}", tx.Tid, ex.Message);
                res = ex.ToString();
                executed = false;
                throw; // TODO: for testing
            }
            finally
            {
                _lockManager.Release(tx);
                if (tx.SourceReplicaId == ReplicaId)
                {
                    txCompleteHandler(tx.Tid, executed, res);
                }
            }
        });
    }

}