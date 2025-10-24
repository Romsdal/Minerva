using System;
using System.Collections.Generic;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;
using ProtoBuf;

namespace Minerva.DB_Server.MinervaLog;

public enum BatchStatus
{
    Active,
    LocalCompleted,
    Available,
    PoASent,
    Committed,
}


public class Batch(int batchId, int sourceReplicaId)
{
    public BatchStatus Status { get; set; } = BatchStatus.Active;
    public int BatchId { get; init; } = batchId;
    public int SourceReplicaId { get; init; } = sourceReplicaId;
    public List<TransactionRecord> Transactions { get; init; } = [];
    private long _size = 0;
    public long Size
    {
        get => _size;
        private set
        {
            _size = value;
        }
    }

    public void Add(ClientRequest Query, MinervaTx tx)
    {
        TransactionRecord txRecord = new()
        {
            Tid = tx.Tid,
            Query = Query,
            WriteSet = tx.WriteSet,
            ReadSet = tx.ReadKeys,
            KeyAccessedFromSnapshot = tx.keyAccessedFromSnapshot,
            PrevTids = tx.PrevTx is null ? [] : [.. tx.PrevTx],
            ConflictStatus = tx.Status == TransactionStatus.DidNotExecute ? TxConflict.NonExecuted : TxConflict.Conflict,
        };
        Transactions.Add(txRecord);
        Interlocked.Add(ref _size, Serializer.Measure(txRecord).Length);
    }




}