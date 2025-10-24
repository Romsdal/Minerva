using MemoryPack;
using ProtoBuf;
using System.Collections.Generic;

namespace Minerva.DB_Server.Network.Protos;


[ProtoContract]
[MemoryPackable]
public partial class WriteSetStore
{
    // YCSB
    [ProtoMember(1)]
    public Dictionary<(int shard, string key), string> YCSBWriteSet { get; set; }

    // TPC-C
    [ProtoMember(2)]
    public Dictionary<long, Warehouse> WarehouseWriteSet { get; set; }

    [ProtoMember(3)]
    public Dictionary<(long DWID, long DID), District> DistrictWriteSet { get; set; }

    [ProtoMember(4)]
    public Dictionary<(long CWID, long CDID, long CID), Customer> CustomerWriteSet { get; set; }

    [ProtoMember(5)]
    public Dictionary<long, Item> ItemWriteSet { get; set; }

    [ProtoMember(6)]
    public Dictionary<(long SWID, long SIID), Stock> StockWriteSet { get; set; }

    [ProtoMember(7)]
    public Dictionary<(long HCID, long HDATE), History> HistoryWriteSet { get; set; }

    [ProtoMember(8)]
    public Dictionary<(long NOWID, long NODID, long NOOID), NewOrder> NewOrderWriteSet { get; set; }

    [ProtoMember(9)]
    public Dictionary<(long OWID, long ODID, long OID), Order> OrderWriteSet { get; set; }

    [ProtoMember(10)]
    public Dictionary<(long OLWID, long OLDID, long OLOID, long OLNUMBER), OrderLine> OrderLineWriteSet { get; set; }
    
    public WriteSetStore()
    {
        YCSBWriteSet = [];
        WarehouseWriteSet = [];
        DistrictWriteSet = [];
        CustomerWriteSet = [];
        ItemWriteSet = [];
        StockWriteSet = [];
        HistoryWriteSet = [];
        NewOrderWriteSet = [];
        OrderWriteSet = [];
        OrderLineWriteSet = [];
    }

}

[ProtoContract]
[MemoryPackable]
public partial class ReadSetStore
{
    // YCSB
    [ProtoMember(1)]
    public List<(int shard, string key)> YCSBReadKeys { get; set; }

    // TPC-C
    [ProtoMember(2)]
    public List<long> WarehouseReadKeys { get; set; }

    [ProtoMember(3)]
    public List<(long DWID, long DID)> DistrictReadKeys { get; set; }

    [ProtoMember(4)]
    public List<(long CWID, long CDID, long CID)> CustomerReadKeys { get; set; }

    [ProtoMember(5)]
    public List<long> ItemReadKeys { get; set; }

    [ProtoMember(6)]
    public List<(long SWID, long SIID)> StockReadKeys { get; set; }

    [ProtoMember(7)]
    public List<(long HCID, long HDATE)> HistoryReadKeys { get; set; }

    [ProtoMember(8)]
    public List<(long NOWID, long NODID, long NOOID)> NewOrderReadKeys { get; set; }

    [ProtoMember(9)]
    public List<(long OWID, long ODID, long OID)> OrderReadKeys { get; set; }

    [ProtoMember(10)]
    public List<(long OLWID, long OLDID, long OLOID, long OLNUMBER)> OrderLineReadKeys { get; set; }
    
    public ReadSetStore()
    {
        YCSBReadKeys = [];
        WarehouseReadKeys = [];
        DistrictReadKeys = [];
        CustomerReadKeys = [];
        ItemReadKeys = [];
        StockReadKeys = [];
        HistoryReadKeys = [];
        NewOrderReadKeys = [];
        OrderReadKeys = [];
        OrderLineReadKeys = [];
    }
}

[ProtoContract]
[MemoryPackable]
public partial class KeyAccessedFromSnapshotStore
{
    // YCSB
    [ProtoMember(1)]
    public Dictionary<(int shard, string key), int> YCSBKeyAccessedFromSnapshot { get; set; }

    // TPC-C
    [ProtoMember(2)]
    public Dictionary<long, int> WarehouseReadsFromSnapshot { get; set; }

    [ProtoMember(3)]
    public Dictionary<(long DWID, long DID), int> DistrictReadsFromSnapshot { get; set; }

    [ProtoMember(4)]
    public Dictionary<(long CWID, long CDID, long CID), int> CustomerReadsFromSnapshot { get; set; }

    [ProtoMember(5)]
    public Dictionary<long, int> ItemReadsFromSnapshot { get; set; }

    [ProtoMember(6)]
    public Dictionary<(long SWID, long SIID), int> StockReadsFromSnapshot { get; set; }

    [ProtoMember(7)]
    public Dictionary<(long HCID, long HDATE), int> HistoryReadsFromSnapshot { get; set; }

    [ProtoMember(8)]
    public Dictionary<(long NOWID, long NODID, long NOOID), int> NewOrderReadsFromSnapshot { get; set; }

    [ProtoMember(9)]
    public Dictionary<(long OWID, long ODID, long OID), int> OrderReadsFromSnapshot { get; set; }

    [ProtoMember(10)]
    public Dictionary<(long OLWID, long OLDID, long OLOID, long OLNUMBER), int> OrderLineReadsFromSnapshot { get; set; }
    
    public KeyAccessedFromSnapshotStore()
    {
        YCSBKeyAccessedFromSnapshot = [];
        WarehouseReadsFromSnapshot = [];
        DistrictReadsFromSnapshot = [];
        CustomerReadsFromSnapshot = [];
        ItemReadsFromSnapshot = [];
        StockReadsFromSnapshot = [];
        HistoryReadsFromSnapshot = [];
        NewOrderReadsFromSnapshot = [];
        OrderReadsFromSnapshot = [];
        OrderLineReadsFromSnapshot = [];
    }
}