using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MemoryPack;
using Minerva.DB_Server.Network.Protos;



namespace Minerva.DB_Server.Storage;

public enum Database
{
    // YCSB
    YCSB,

    // TPC-C
    Warehouse,
    District,
    Customer,
    Item,
    Stock,
    History,
    NewOrder,
    Order,
    OrderLine
}




[MemoryPackable]
// Generic entry to hold value and last-updated commit id (Cid)
public partial class PersistEntry<T>
{
    public T Value;
    public int Cid;

    public PersistEntry(T value, int cid)
    {
        Value = value;
        Cid = cid;
    }
}

public class PersistentStorage
{

    // YCSB
    // (shard, key) -> value
    private Dictionary<int, Dictionary<string, PersistEntry<string>>> YCSBStore = [];

    // TPC-C
    private Dictionary<long, PersistEntry<Warehouse>> WarehouseStore = [];
    // District: (W_ID, D_ID) -> District
    private Dictionary<long, Dictionary<long, PersistEntry<District>>> DistrictStore = [];
    // Customer: (W_ID, D_ID, C_ID) -> Customer
    private Dictionary<long, Dictionary<(long CDID, long CID), PersistEntry<Customer>>> CustomerStore = [];

    private Dictionary<long, PersistEntry<Item>> ItemStore = [];
    // Stock: (S_W_ID, S_I_ID) -> Stock
    private Dictionary<long, Dictionary<long, PersistEntry<Stock>>> StockStore = [];
    // History: (HCID, HDATE) -> History
    private Dictionary<long, Dictionary<long, PersistEntry<History>>> HistoryStore = [];
    // NewOrder: (NO_W_ID, NO_D_ID, NO_O_ID) -> NewOrder
    private Dictionary<long, Dictionary<(long NODID, long NOOID), PersistEntry<NewOrder>>> NewOrderStore = [];
    // Order: (O_W_ID, O_D_ID, O_ID) -> Order
    private Dictionary<long, Dictionary<(long ODID, long OID), PersistEntry<Order>>> OrderStore = [];
    // OrderLine: (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER) -> OrderLine
    private Dictionary<long, Dictionary<(long OLDID, long OLOID, long OLNUMBER), PersistEntry<OrderLine>>> OrderLineStore = [];

    // for customer last name index
    public Dictionary<string, HashSet<(long CWID, long CDID, long CID)>> CustomerIndexByLastName = [];
    // For stale tracking
    public HashSet<(int rid, int tid)> ReExOriginalTransactions = [];

    public ReaderWriterLockSlim readerWriterLockSlim = new();

    public bool Get<TKey>(Database database, TKey key, out object entry)
    {
        readerWriterLockSlim.EnterReadLock();

        try
        {
            switch (database)
            {
                case Database.YCSB:
                    if (key is ValueTuple<int, string> ycsbKey)
                    {
                        var (rid, keyString) = ycsbKey;
                        if (YCSBStore.TryGetValue(rid, out var ycsbByKey))
                        {
                            var found = ycsbByKey.TryGetValue(keyString, out var ycsbEntry);
                            entry = ycsbEntry;
                            return found;
                        }
                    }
                    break;
                case Database.Warehouse:
                    if (key is long wId)
                    {
                        var foundW = WarehouseStore.TryGetValue(wId, out var warehouseEntry);
                        entry = warehouseEntry;
                        return foundW;
                    }

                    break;

                case Database.District:
                    if (key is ValueTuple<long, long> districtKey)
                    {
                        var (DWID, DID) = districtKey;
                        if (DistrictStore.TryGetValue(DWID, out var districtByDId))
                        {
                            var foundD = districtByDId.TryGetValue(DID, out var districtEntry);
                            entry = districtEntry;
                            return foundD;
                        }
                    }
                    break;
                case Database.Customer:
                    if (key is ValueTuple<long, long, long> customerKey)
                    {
                        var (CWID, CDID, CID) = customerKey;
                        if (CustomerStore.TryGetValue(CWID, out var customerByDId))
                        {
                            var foundC = customerByDId.TryGetValue((CDID, CID), out var customerEntry);
                            entry = customerEntry;
                            return foundC;
                        }
                    }
                    break;
                case Database.Item:
                    if (key is long iId)
                    {
                        var foundI = ItemStore.TryGetValue(iId, out var itemEntry);
                        entry = itemEntry;
                        return foundI;
                    }
                    break;
                case Database.Stock:
                    if (key is ValueTuple<long, long> stockKey)
                    {
                        var (SWID, SIID) = stockKey;
                        if (StockStore.TryGetValue(SWID, out var stockByIId))
                        {
                            var foundS = stockByIId.TryGetValue(SIID, out var stockEntry);
                            entry = stockEntry;
                            return foundS;
                        }
                    }
                    break;
                case Database.History:
                    if (key is ValueTuple<long, long> historyKey)
                    {
                        var (HCID, HDATE) = historyKey;
                        if (HistoryStore.TryGetValue(HCID, out var historyByDate))
                        {
                            var foundH = historyByDate.TryGetValue(HDATE, out var historyEntry);
                            entry = historyEntry;
                            return foundH;
                        }
                    }
                    break;
                case Database.NewOrder:
                    if (key is ValueTuple<long, long, long> newOrderKey)
                    {
                        var (NOWID, NODID, NOOID) = newOrderKey;
                        if (NewOrderStore.TryGetValue(NOWID, out var newOrderByDId))
                        {
                            var foundNO = newOrderByDId.TryGetValue((NODID, NOOID), out var newOrderEntry);
                            entry = newOrderEntry;
                            return foundNO;
                        }
                    }
                    break;
                case Database.Order:
                    if (key is ValueTuple<long, long, long> orderKey)
                    {
                        var (OWID, ODID, OID) = orderKey;
                        if (OrderStore.TryGetValue(OWID, out var orderByDId))
                        {
                            var foundO = orderByDId.TryGetValue((ODID, OID), out var orderEntry);
                            entry = orderEntry;
                            return foundO;
                        }
                    }
                    break;
                case Database.OrderLine:
                    if (key is ValueTuple<long, long, long, long> orderLineKey)
                    {
                        var (OLWID, OLDID, OLOID, OLNUMBER) = orderLineKey;
                        if (OrderLineStore.TryGetValue(OLWID, out var orderLineByOId))
                        {
                            var foundOL = orderLineByOId.TryGetValue((OLDID, OLOID, OLNUMBER), out var orderLineEntry);
                            entry = orderLineEntry;
                            return foundOL;
                        }
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(database), database, null);

            }

        }
        finally
        {
            readerWriterLockSlim.ExitReadLock();
        }

        entry = null;
        return false;
    }


    public bool Set<TKey>(Database database, TKey key, object value)
    {

        switch (database)
        {
            case Database.YCSB:
                if (key is ValueTuple<int, string> ycsbKey && value is PersistEntry<string> ycsbValue)
                {
                    var (rid, keyString) = ycsbKey;
                    if (!YCSBStore.TryGetValue(rid, out var ycsbPart))
                    {
                        ycsbPart = [];
                        YCSBStore[rid] = ycsbPart;
                    }
                    ycsbPart[keyString] = ycsbValue;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for YCSB. Expected (int shard, string key) and string value.");
                }

            case Database.Warehouse:
                if (key is long wId && value is PersistEntry<Warehouse> warehouse)
                {
                    WarehouseStore[wId] = warehouse;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for Warehouse. Expected long W_ID and Warehouse value.");
                }

            case Database.District:
                if (key is ValueTuple<long, long> districtKey && value is PersistEntry<District> district)
                {
                    var (DWID, DID) = districtKey;
                    if (!DistrictStore.TryGetValue(DWID, out var districtByDId))
                    {
                        districtByDId = [];
                        DistrictStore[DWID] = districtByDId;
                    }
                    districtByDId[DID] = district;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for District. Expected (long, long) key and District value.");
                }

            case Database.Customer:
                if (key is ValueTuple<long, long, long> customerKey && value is PersistEntry<Customer> customer)
                {
                    var (CWID, CDID, CID) = customerKey;
                    if (!CustomerStore.TryGetValue(CWID, out var customerByDId))
                    {
                        customerByDId = [];
                        CustomerStore[CWID] = customerByDId;
                    }
                    customerByDId[(CDID, CID)] = customer;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for Customer. Expected (long, long, long) key and Customer value.");
                }

            case Database.Item:
                if (key is long iId && value is PersistEntry<Item> item)
                {
                    ItemStore[iId] = item;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for Item. Expected long I_ID and Item value.");
                }

            case Database.Stock:
                if (key is ValueTuple<long, long> stockKey && value is PersistEntry<Stock> stock)
                {
                    var (SWID, SIID) = stockKey;
                    if (!StockStore.TryGetValue(SWID, out var stockByIId))
                    {
                        stockByIId = [];
                        StockStore[SWID] = stockByIId;
                    }
                    stockByIId[SIID] = stock;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for Stock. Expected (long, long) key and Stock value.");
                }

            case Database.History:
                if (key is ValueTuple<long, long> historyKey && value is PersistEntry<History> history)
                {
                    var (HCID, HDATE) = historyKey;
                    if (!HistoryStore.TryGetValue(HCID, out var historyByDate))
                    {
                        historyByDate = [];
                        HistoryStore[HCID] = historyByDate;
                    }
                    historyByDate[HDATE] = history;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for History. Expected (long, long) key and History value.");
                }

            case Database.NewOrder:
                if (key is ValueTuple<long, long, long> newOrderKey && value is PersistEntry<NewOrder> newOrder)
                {
                    var (NOWID, NODID, NOOID) = newOrderKey;
                    if (!NewOrderStore.TryGetValue(NOWID, out var newOrderByDId))
                    {
                        newOrderByDId = [];
                        NewOrderStore[NOWID] = newOrderByDId;
                    }
                    newOrderByDId[(NODID, NOOID)] = newOrder;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for NewOrder. Expected (long, long, long) key and NewOrder value.");
                }

            case Database.Order:
                if (key is ValueTuple<long, long, long> orderKey && value is PersistEntry<Order> order)
                {
                    var (OWID, ODID, OID) = orderKey;
                    if (!OrderStore.TryGetValue(OWID, out var orderByDId))
                    {
                        orderByDId = [];
                        OrderStore[OWID] = orderByDId;
                    }
                    orderByDId[(ODID, OID)] = order;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for Order. Expected (long, long, long) key and Order value.");
                }

            case Database.OrderLine:
                if (key is ValueTuple<long, long, long, long> orderLineKey && value is PersistEntry<OrderLine> orderLine)
                {
                    var (OLWID, OLDID, OLOID, OLNUMBER) = orderLineKey;
                    if (!OrderLineStore.TryGetValue(OLWID, out var orderLineByOId))
                    {
                        orderLineByOId = [];
                        OrderLineStore[OLWID] = orderLineByOId;
                    }
                    orderLineByOId[(OLDID, OLOID, OLNUMBER)] = orderLine;
                    return true;
                }
                else
                {
                    throw new ArgumentException("Invalid key/value type for OrderLine. Expected (long, long, long, long) key and OrderLine value.");
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(database), database, null);
        }

    }



    public void SaveStorageToDisk(string dirPath)
    {
        Directory.CreateDirectory(dirPath);

        SerializeToFile(dirPath, "YCSBStore.bin", YCSBStore);
        SerializeToFile(dirPath, "WarehouseStore.bin", WarehouseStore);
        SerializeToFile(dirPath, "DistrictStore.bin", DistrictStore);
        SerializeToFile(dirPath, "CustomerStore.bin", CustomerStore);
        SerializeToFile(dirPath, "ItemStore.bin", ItemStore);
        SerializeToFile(dirPath, "StockStore.bin", StockStore);
        SerializeToFile(dirPath, "HistoryStore.bin", HistoryStore);
        SerializeToFile(dirPath, "NewOrderStore.bin", NewOrderStore);
        SerializeToFile(dirPath, "OrderStore.bin", OrderStore);
        SerializeToFile(dirPath, "OrderLineStore.bin", OrderLineStore);
        SerializeToFile(dirPath, "CustomerIndexByLastName.bin", CustomerIndexByLastName);
    }

    public async Task LoadStorageFromDisk(string dirPath, string[] databasesToLoad)
    {
        var tasks = new List<Task>();
        Task<Dictionary<int, Dictionary<string, PersistEntry<string>>>> ycsbTask = null;
        Task<Dictionary<long, PersistEntry<Warehouse>>> warehouseTask = null;
        Task<Dictionary<long, Dictionary<long, PersistEntry<District>>>> districtTask = null;
        Task<Dictionary<long, Dictionary<(long, long), PersistEntry<Customer>>>> customerTask = null;
        Task<Dictionary<long, PersistEntry<Item>>> itemTask = null;
        Task<Dictionary<long, Dictionary<long, PersistEntry<Stock>>>> stockTask = null;
        Task<Dictionary<long, Dictionary<long, PersistEntry<History>>>> historyTask = null;
        Task<Dictionary<long, Dictionary<(long, long), PersistEntry<NewOrder>>>> newOrderTask = null;
        Task<Dictionary<long, Dictionary<(long, long), PersistEntry<Order>>>> orderTask = null;
        Task<Dictionary<long, Dictionary<(long, long, long), PersistEntry<OrderLine>>>> orderLineTask = null;
        Task<Dictionary<string, HashSet<(long, long, long)>>> customerIndexTask = null;

        if (databasesToLoad.Contains("YCSB"))
        {
            ycsbTask = DeserializeFromFile<Dictionary<int, Dictionary<string, PersistEntry<string>>>>(dirPath, "YCSBStore.bin");
            tasks.Add(ycsbTask);
        }

        if (databasesToLoad.Contains("TPCC"))
        {
            warehouseTask = DeserializeFromFile<Dictionary<long, PersistEntry<Warehouse>>>(dirPath, "WarehouseStore.bin");
            districtTask = DeserializeFromFile<Dictionary<long, Dictionary<long, PersistEntry<District>>>>(dirPath, "DistrictStore.bin");
            customerTask = DeserializeFromFile<Dictionary<long, Dictionary<(long, long), PersistEntry<Customer>>>>(dirPath, "CustomerStore.bin");
            itemTask = DeserializeFromFile<Dictionary<long, PersistEntry<Item>>>(dirPath, "ItemStore.bin");
            stockTask = DeserializeFromFile<Dictionary<long, Dictionary<long, PersistEntry<Stock>>>>(dirPath, "StockStore.bin");
            historyTask = DeserializeFromFile<Dictionary<long, Dictionary<long, PersistEntry<History>>>>(dirPath, "HistoryStore.bin");
            newOrderTask = DeserializeFromFile<Dictionary<long, Dictionary<(long, long), PersistEntry<NewOrder>>>>(dirPath, "NewOrderStore.bin");
            orderTask = DeserializeFromFile<Dictionary<long, Dictionary<(long, long), PersistEntry<Order>>>>(dirPath, "OrderStore.bin");
            orderLineTask = DeserializeFromFile<Dictionary<long, Dictionary<(long, long, long), PersistEntry<OrderLine>>>>(dirPath, "OrderLineStore.bin");
            customerIndexTask = DeserializeFromFile<Dictionary<string, HashSet<(long, long, long)>>>(dirPath, "CustomerIndexByLastName.bin");
            tasks.AddRange(new Task[] {
                warehouseTask, districtTask, customerTask, itemTask, stockTask,
                historyTask, newOrderTask, orderTask, orderLineTask, customerIndexTask
            });
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        if (ycsbTask != null) YCSBStore = await ycsbTask.ConfigureAwait(false);
        if (warehouseTask != null) WarehouseStore = await warehouseTask.ConfigureAwait(false);
        if (districtTask != null) DistrictStore = await districtTask.ConfigureAwait(false);
        if (customerTask != null) CustomerStore = await customerTask.ConfigureAwait(false);
        if (itemTask != null) ItemStore = await itemTask.ConfigureAwait(false);
        if (stockTask != null) StockStore = await stockTask.ConfigureAwait(false);
        if (historyTask != null) HistoryStore = await historyTask.ConfigureAwait(false);
        if (newOrderTask != null) NewOrderStore = await newOrderTask.ConfigureAwait(false);
        if (orderTask != null) OrderStore = await orderTask.ConfigureAwait(false);
        if (orderLineTask != null) OrderLineStore = await orderLineTask.ConfigureAwait(false);
        if (customerIndexTask != null) CustomerIndexByLastName = await customerIndexTask.ConfigureAwait(false);
    }

    private static void SerializeToFile<T>(string directory, string fileName, T data)
    {
        var path = Path.Combine(directory, fileName);
        using var stream = File.Create(path);
        MemoryPackSerializer.SerializeAsync(stream, data).AsTask().GetAwaiter().GetResult();
    }

    private static async Task<T> DeserializeFromFile<T>(string directory, string fileName)
    {
        var path = Path.Combine(directory, fileName);
        using var stream = File.OpenRead(path);
        return await MemoryPackSerializer.DeserializeAsync<T>(stream) ?? throw new InvalidDataException($"Failed to deserialize {fileName}");
    }
}