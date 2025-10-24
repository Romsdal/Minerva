using System.Collections.Generic;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Transactions;



public class ReadsTimeStamp
{
    // YCSB
    public Dictionary<(int shard, string key), long> YCSBReadsTimeStamp = [];

    // TPC-C
    public Dictionary<long, long> WarehouseReadsTimeStamp = [];
    public Dictionary<(long DWID, long DID), long> DistrictReadsTimeStamp = [];
    public Dictionary<(long CWID, long CDID, long CID), long> CustomerReadsTimeStamp = [];
    public Dictionary<long, long> ItemReadsTimeStamp = [];
    public Dictionary<(long SWID, long SIID), long> StockReadsTimeStamp = [];
    public Dictionary<(long HCID, long HDATE), long> HistoryReadsTimeStamp = [];
    public Dictionary<(long NOWID, long NODID, long NOOID), long> NewOrderReadsTimeStamp = [];
    public Dictionary<(long OWID, long ODID, long OID), long> OrderReadsTimeStamp = [];
    public Dictionary<(long OLWID, long OLDID, long OLOID, long OLNUMBER), long> OrderLineReadsTimeStamp = [];

    public ReadSetStore ReadsTimeStampToReadSet()
    {
        var res = new ReadSetStore
        {
            YCSBReadKeys = [],
            WarehouseReadKeys = [],
            DistrictReadKeys = [],
            CustomerReadKeys = [],
            ItemReadKeys = [],
            StockReadKeys = [],
            HistoryReadKeys = [],
            NewOrderReadKeys = [],
            OrderReadKeys = [],
            OrderLineReadKeys = []
        };



        res.YCSBReadKeys.AddRange(YCSBReadsTimeStamp.Keys);
        res.WarehouseReadKeys.AddRange(WarehouseReadsTimeStamp.Keys);
        res.DistrictReadKeys.AddRange(DistrictReadsTimeStamp.Keys);
        res.CustomerReadKeys.AddRange(CustomerReadsTimeStamp.Keys);
        res.ItemReadKeys.AddRange(ItemReadsTimeStamp.Keys);
        res.StockReadKeys.AddRange(StockReadsTimeStamp.Keys);
        res.HistoryReadKeys.AddRange(HistoryReadsTimeStamp.Keys);
        res.NewOrderReadKeys.AddRange(NewOrderReadsTimeStamp.Keys);
        res.OrderReadKeys.AddRange(OrderReadsTimeStamp.Keys);
        res.OrderLineReadKeys.AddRange(OrderLineReadsTimeStamp.Keys);

        return res;
    }
}
