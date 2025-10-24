using System;
using System.Threading;
using Minerva.DB_Server.Storage;

namespace Minerva.DB_Server.Transactions;

public class MinervaTxManager : ITransactionManager
{
    private readonly PersistentStorage _persistedDb;
    public TempStorage TemporaryState { get; set; }
    public int NextTransactionId;
    private readonly TimeProvider _timeProvider;
    private ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);

    public MinervaTxManager(PersistentStorage persistedDb, TimeProvider timeProvider = null)
    {
        _persistedDb = persistedDb;
        NextTransactionId = 1; // Start transaction IDs from 1
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Transaction CreateTransaction()
    {
        if (TemporaryState == null)
        {
            throw new InvalidOperationException("Temporary state must be set before creating a transaction.");
        }

        var transaction = new MinervaTx(Interlocked.Increment(ref NextTransactionId), _persistedDb, TemporaryState, _timeProvider, _rwLock); 
        return transaction;
    }
}