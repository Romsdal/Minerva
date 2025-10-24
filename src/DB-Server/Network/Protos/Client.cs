using System;
using System.Collections.Generic;
using MemoryPack;
using ProtoBuf;

namespace Minerva.DB_Server.Network.Protos;

public enum QueryType
{
    Stop = 1,
    Stats = 2,
    SaveState = 3,
    Ycsb = 4,
    Tpccno = 5, // new order
    Tpccos = 6, // order status
    Tpccp = 7, // payment
    Tpccsl = 8, // stock level
    Tpccd = 9, // delivery
    Tpccli = 10, // load item
    Tpcclw = 11, // load warehouse
    Tpccld = 12, // load district
    Tpcclc = 13, // load customer
    Tpccls = 14, // load stock
    Tpcclio = 15, // load initial orders
    Tpcclh = 16, // load history
}

public enum OpType
{
    Set = 1,
    Get = 2,
    Delete = 3
}

[ProtoContract]
[MemoryPackable]
public partial class TxResult
{   
    [ProtoMember(1)]
    public uint SeqId { get; set; } 
    [ProtoMember(2)]
    public bool Executed { get; set; }
    [ProtoMember(3)]
    public string TxResultStr { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class ClientRequest
{
    [ProtoMember(1)]
    public QueryType Type { get; set; }
    
    [ProtoMember(2)]
    public uint SeqId { get; set; } 
    
    // YCSB
    [ProtoMember(3)]
    public List<KV> KVCmds { get; set; }

    // TPC-C
    // query
    [ProtoMember(4)]
    public TPCCNO Tpccno { get; set; }
    
    [ProtoMember(5)]
    public TPCCP Tpccp { get; set; }

    // load
    [ProtoMember(6)]
    public TPCCLI Tpccli { get; set; }
    
    [ProtoMember(7)]
    public TPCCLW Tpcclw { get; set; }
    
    [ProtoMember(8)]
    public TPCCLD Tpccld { get; set; }
    
    [ProtoMember(9)]
    public TPCCLC Tpcclc { get; set; }
    
    [ProtoMember(10)]
    public TPCCLS Tpccls { get; set; }
    
    [ProtoMember(11)]
    public TPCCLIO Tpcclio { get; set; }
    
    [ProtoMember(12)]
    public TPCCLH Tpcclh { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class KV
{
    [ProtoMember(1)]
    public OpType Type { get; set; }

    [ProtoMember(2)]
    public int Shard { get; set; }

    [ProtoMember(3)]
    public string Key { get; set; }
    
    [ProtoMember(4)]
    public string Value { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCNO
{
    [ProtoMember(1)]
    public int W_ID { get; set; }
    
    [ProtoMember(2)]
    public int D_ID { get; set; }
    
    [ProtoMember(3)]
    public int C_ID { get; set; }
    
    [ProtoMember(4)]
    public int O_ID { get; set; }
    
    [ProtoMember(5)]
    public List<NOItem> Items { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class NOItem
{
    [ProtoMember(1)]
    public int I_ID { get; set; }
    
    [ProtoMember(2)]
    public int W_ID { get; set; }
    
    [ProtoMember(3)]
    public int Q { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCP
{
    [ProtoMember(1)]
    public int WID { get; set; }
    
    [ProtoMember(2)]
    public int DID { get; set; }
    
    [ProtoMember(3)]
    public int CID { get; set; } // Customer ID (optional)
    
    [ProtoMember(4)]
    public string CLAST { get; set; } // Customer Last Name (optional)
    
    [ProtoMember(5)]
    public long AMOUNT { get; set; } // Payment Amount
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCLI
{
    [ProtoMember(1)]
    public int I { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCLW
{
    [ProtoMember(1)]
    public int W { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCLD
{
    [ProtoMember(1)]
    public int D { get; set; }
    
    [ProtoMember(2)]
    public int WarehouseId { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCLC
{
    [ProtoMember(1)]
    public int WarehouseId { get; set; }
    
    [ProtoMember(2)]
    public int DistrictID { get; set; }
    
    [ProtoMember(3)]
    public int C { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCLS
{
    [ProtoMember(1)]
    public int I { get; set; }
    
    [ProtoMember(2)]
    public int WarehouseId { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCLIO
{
    [ProtoMember(1)]
    public int D { get; set; }
    
    [ProtoMember(2)]
    public int W { get; set; }
    
    [ProtoMember(3)]
    public int O { get; set; }
}

[ProtoContract]
[MemoryPackable]
public partial class TPCCLH
{
    [ProtoMember(1)]
    public int D { get; set; }
    
    [ProtoMember(2)]
    public int W { get; set; }
    
    [ProtoMember(3)]
    public int C { get; set; }
}