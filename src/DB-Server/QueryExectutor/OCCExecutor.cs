using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.QueryExecutor;



public class MinervaTxOCCExecutor
{
    private readonly IQueryParser _queryParser;
    private readonly MinervaTxManager _transactionManager;
    private readonly HighContentionTxManager _highContentionTxManager;
    private readonly ILogger _logger = LoggerManager.GetLogger();

    private ReaderWriterLockSlim SetNewStateLock = new(LockRecursionPolicy.NoRecursion);

    public MinervaTxOCCExecutor(IQueryParser queryParser, PersistentStorage persistedDB)
    {
        _transactionManager = new MinervaTxManager(persistedDB);
        _highContentionTxManager = new HighContentionTxManager(persistedDB, _transactionManager);
        _queryParser = queryParser;
    }

    public void SetNewTempStates(List<string> databases = null)
    {
        if (databases is not null)
        {
            var tempState = new TempStorage();
            _transactionManager.TemporaryState = tempState;
        }
        else
        {
            SetNewStateLock.EnterWriteLock();
            try
            {
                _transactionManager.TemporaryState.Clear();
            }
            finally
            {
                SetNewStateLock.ExitWriteLock();
            }
        }
    }

    public MinervaTx OCCExecuteSingle(ClientRequest query, bool highContentionMode)
    {
        if (!highContentionMode)
        {
            IExecutionPlan plan = _queryParser.ParseSQL(query, _transactionManager);
            MinervaTx tx = null;
            while (true)
            {
                SetNewStateLock.EnterReadLock();
                try
                {
                    tx = (MinervaTx)plan.Execute();
                    return tx;
                }
                catch (TransactionFailedException)
                {
                    ////_logger.LogTrace("Transaction failed due to a conflict. Retrying...");
                }
                catch (TransactionAbortedException)
                {
                    ////_logger.LogTrace("Transaction aborted by user request.");
                    return tx;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An unexpected error occurred during OCC execution.");
                    throw; // For testing, re-throw the exception
                }
                finally
                {
                    SetNewStateLock.ExitReadLock();
                }
            }
        }
        else
        {
            IExecutionPlan plan = _queryParser.ParseSQL(query, _highContentionTxManager);
            return (HighContentionTx)plan.Execute();
        }
    }
}