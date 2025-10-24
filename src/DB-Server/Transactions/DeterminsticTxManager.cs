using System;
using Minerva.DB_Server.Storage;

namespace Minerva.DB_Server.Transactions;

public class DeterministicTxManager: ITransactionManager
{
    private readonly PersistentStorage _persistedDb;
    private int _nextTransactionId;
    private int _curEpoch;

    public DeterministicTxManager(PersistentStorage persistedDb, int curEpoch)
    {
        _persistedDb = persistedDb;
        _nextTransactionId = 1; // Start transaction IDs from 1
        _curEpoch = curEpoch;
    }


    public Transaction CreateTransaction()
    {
        return new DeterministicTx(_nextTransactionId++, _persistedDb, _curEpoch);
    }
}