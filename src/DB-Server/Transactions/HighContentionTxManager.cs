using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Minerva.DB_Server.Storage;

namespace Minerva.DB_Server.Transactions;

public class HighContentionTxManager : ITransactionManager
{
    private readonly PersistentStorage _persistedDb;
    private readonly MinervaTxManager _mtm;

    public HighContentionTxManager(PersistentStorage persistedDb, MinervaTxManager mtm)
    {
        _persistedDb = persistedDb;
        _mtm = mtm;
    }

    public Transaction CreateTransaction()
    {
        var transaction = new HighContentionTx(Interlocked.Increment(ref _mtm.NextTransactionId), _persistedDb);
        return transaction;
    }
}