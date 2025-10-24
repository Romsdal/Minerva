using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.Interface;

public class QueryHandler
{

    private readonly PersistentStorage persistentStorage;
    private readonly MinervaTxOCCExecutor _occExecutor;
    private readonly GlobalLogManager _logManager;

    // // We do this because in re-execution, a new transaction object is created
    private readonly ConcurrentDictionary<int, TaskCompletionSource<(bool executed, string result)>> _transactionWaitHandles = new();
    private readonly ILogger _logger = LoggerManager.GetLogger();

    public QueryHandler(MinervaTxOCCExecutor occExecutor, GlobalLogManager logManager, PersistentStorage pdb)
    {
        _occExecutor = occExecutor;
        _logManager = logManager;
        persistentStorage = pdb;
    }


    public async Task<(bool executed, string result)> ReceivedQuery(ClientRequest query)
    {
        //_logger.("Received query: {Query}", query);
        try
        {
            var tx = _occExecutor.OCCExecuteSingle(query, _logManager.HiContentionMode);

            if (tx.Status == TransactionStatus.LocalCompleted || tx.Status == TransactionStatus.DidNotExecute)
            {
                Interlocked.Increment(ref Stats.TotalLocalExecuted);
                //////_logger.LogTrace("Query with {seqId} assigned with {TransactionId} executed locally.", query.SeqId, tx.Tid);

                if (!_transactionWaitHandles.TryAdd(tx.Tid, new TaskCompletionSource<(bool, string)>()))
                {
                    _logger.LogError("Transaction with ID {TransactionId} already exists in results.", tx.Tid);
                    throw new InvalidOperationException($"Transaction with ID {tx.Tid} already exists in results.");
                }

                int tid = tx.Tid;

                _logManager.TxComplete(query, tx);

                var (executed, result) = await _transactionWaitHandles[tid].Task;

                //////_logger.LogTrace("Transaction {TransactionId} for query {seqId} committed", tx.Tid, query.SeqId);
                _transactionWaitHandles.TryRemove(tid, out _);

                if (result is not null) // is an re-executed transaction, return the new result
                {
                    return (executed, result);
                }
                else
                {
                    return (true, tx.Result); // return the original result
                }
            }
            else
            {
                _logger.LogWarning("Transaction {TransactionId} did not complete locally, status: {Status}", tx.Tid, tx.Status);
                return (false, tx.Result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing query: {Query}, {ex}", query, ex.Message);
            return (false, ex.ToString());
        }
        
    }

    /// <summary>
    /// Notify the client when a transaction is committed.
    /// If result is not null, then it is a re-executed transaction, overwrite the existing results
    /// </summary>
    public void NotifyTransactionCommitted(int tid, bool executed = false, string result = null)
    {
        _transactionWaitHandles[tid].SetResult((executed, result));
    }


    public void SaveStorageToDisk()
    {
        persistentStorage.SaveStorageToDisk("/mnt/data/minerva_data/");
        _logger.LogInformation("Storage saved to disk.");
    }
}