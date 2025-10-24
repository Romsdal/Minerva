using MemoryPack;
using ProtoBuf;
using System.Collections.Generic;

namespace Minerva.DB_Server.Network.Protos;

public enum GrpcBatchStatus
{
    Active = 0,
    LocalCompleted = 1,
    Available = 2,
    PoASent = 3,
    Committed = 4
}


public enum TxConflict
{
    Conflict = 0,
    None = 1,
    Stale = 2,
    NonExecuted = 3
}

[ProtoContract]
[MemoryPackable]
public partial class TransactionRecord
{
    [ProtoMember(1)]
    public int Tid { get; set; }

    [ProtoMember(2)]
    public List<int> PrevTids { get; set; }

    [ProtoMember(3)]
    public ClientRequest Query { get; set; }

    [ProtoMember(4)]
    public WriteSetStore WriteSet { get; set; }

    [ProtoMember(5)]
    public ReadSetStore ReadSet { get; set; }

    [ProtoMember(6)]
    public KeyAccessedFromSnapshotStore KeyAccessedFromSnapshot { get; set; }

    // default to conflict, because ConflictGraphSolver returns only the non conflict ones.
    public TxConflict ConflictStatus = TxConflict.Conflict;
    // used for determinstic re-execution
    public int SourceReplicaId;

    
    public TransactionRecord()
    {
        PrevTids = [];
    }

}

[ProtoContract]
[MemoryPackable]
public partial class BatchMsg
{
    [ProtoMember(1)]
    public GrpcBatchStatus Status { get; set; }
    
    [ProtoMember(2)]
    public int BatchId { get; set; }
    
    [ProtoMember(3)]
    public int SourceReplicaId { get; set; }
    
    [ProtoMember(4)]
    public List<TransactionRecord> Transactions { get; set; }
}