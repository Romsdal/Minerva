using MemoryPack;
using ProtoBuf;
using System.Collections.Generic;

namespace Minerva.DB_Server.Network.Protos;


[ProtoContract]
[MemoryPackable]
public partial class BatchAckMsg
{
    [ProtoMember(1)]
    public int SourceReplicaId { get; set; }

    [ProtoMember(2)]
    public int BatchId { get; set; }

    [ProtoMember(3)]
    public int FromId { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class PoAMsg
{
    [ProtoMember(1)]
    public int SourceReplicaId { get; set; }

    [ProtoMember(2)]
    public int BatchId { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class BatchRequestMsg
{
    [ProtoMember(1)]
    public int SourceReplicaId { get; set; }
    
    [ProtoMember(2)]
    public int BatchId { get; set; }
    
    [ProtoMember(3)]
    public int FromId { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class ConsistentCutMsg
{
    [ProtoMember(1)]
    public List<int> ConsistentCutIndices { get; set; }
    
    [ProtoMember(2)]
    public int EpochId { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class ReplicaEpochAnnoMsg
{
    [ProtoMember(1)]
    public int ReplicaId { get; set; }
    
    [ProtoMember(2)]
    public int EpochId { get; set; }
}

