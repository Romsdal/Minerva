using System;
using System.ComponentModel;
using MemoryPack;
using ProtoBuf;

namespace Minerva.DB_Server.Network.Protos;

[ProtoContract]
[MemoryPackable]
public partial class PingRequest
{
    [ProtoMember(1)]
    public int NodeId { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class PingReply
{

    [ProtoMember(1)]
    public int NodeId { get; set; }
}