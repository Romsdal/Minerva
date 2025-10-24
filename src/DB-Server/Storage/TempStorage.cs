using System.Collections.Generic;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Storage;



public class TempStorage
{
    // Generic entry to collapse value + metadata into a single allocation per key
    public sealed class TempEntry<T>
    {
        public T Value;
        public long Ts;
        public int Tid;

        public TempEntry(T value, long ts, int tid)
        {
            Value = value;
            Ts = ts;
            Tid = tid;
        }
    }

    // YCSB
    public readonly Dictionary<(int shard, string key), TempEntry<string>> YCSBStore = [];
    // TPC-C
    public readonly Dictionary<long, TempEntry<Warehouse>> WarehouseStore = [];
    
    public readonly Dictionary<(long DWID, long DID), TempEntry<District>> DistrictStore = [];

    public readonly Dictionary<(long CWID, long CDID, long CID), TempEntry<Customer>> CustomerStore = [];

    public readonly Dictionary<long, TempEntry<Item>> ItemStore = [];

    public readonly Dictionary<(long SWID, long SIID), TempEntry<Stock>> StockStore = [];

    public readonly Dictionary<(long HCID, long HDATE), TempEntry<History>> HistoryStore = [];

    public readonly Dictionary<(long NOWID, long NODID, long NOOID), TempEntry<NewOrder>> NewOrderStore = [];

    public readonly Dictionary<(long OWID, long ODID, long OID), TempEntry<Order>> OrderStore = [];

    public readonly Dictionary<(long OLWID, long OLDID, long OLOID, long OLNUMBER), TempEntry<OrderLine>> OrderLineStore = [];


    public void Clear()
    {
        YCSBStore.Clear();

        WarehouseStore.Clear();

        DistrictStore.Clear();

        CustomerStore.Clear();

        ItemStore.Clear();

        StockStore.Clear();

        HistoryStore.Clear();

        NewOrderStore.Clear();

        OrderStore.Clear();

        OrderLineStore.Clear();
    }
}