using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;


namespace Minerva.DB_Server.Transactions;

public class MinervaTx : Transaction
{
    public WriteSetStore WriteSet = new();
    public ReadSetStore ReadKeys = new();
    public KeyAccessedFromSnapshotStore keyAccessedFromSnapshot = new();

    private readonly ReadsTimeStamp _reads = new();
    private readonly TimeProvider _timer;
    private readonly ReaderWriterLockSlim _rwLock;


    private readonly PersistentStorage _persistDb;
    private readonly TempStorage _tempState;

    public HashSet<int> PrevTx { get; init; } = [];

    // For all the keys that were read by a transaction in this batch,
    // record the snapshot that the key was last updated in.
    // This is used to determine if a transaction is stale or not.
    public MinervaTx(int tid, PersistentStorage persistDb, TempStorage tempState, TimeProvider timer, ReaderWriterLockSlim rwLock) : base(tid)
    {
        _persistDb = persistDb;
        _tempState = tempState;
        _timer = timer;
        _rwLock = rwLock;
    }

    public override void Abort()
    {
        Status = TransactionStatus.Aborted;
        throw new TransactionAbortedException($"Transaction {Tid} aborted by user request.");
    }

    public override void Begin()
    {
        Status = TransactionStatus.Active;
    }

    public override void Complete()
    {

        if (Status != TransactionStatus.Active)
        {
            throw new InvalidOperationException($"Transaction {Tid} is not active. Cannot complete.");
        }

        // OCC style validation using timestamps
        _rwLock.EnterWriteLock();

        try
        {
            if (!CheckNoConflict(WriteSet.YCSBWriteSet, _reads.YCSBReadsTimeStamp, _tempState.YCSBStore) ||
                        !CheckNoConflict(WriteSet.WarehouseWriteSet, _reads.WarehouseReadsTimeStamp, _tempState.WarehouseStore) ||
                        !CheckNoConflict(WriteSet.DistrictWriteSet, _reads.DistrictReadsTimeStamp, _tempState.DistrictStore) ||
                        !CheckNoConflict(WriteSet.CustomerWriteSet, _reads.CustomerReadsTimeStamp, _tempState.CustomerStore) ||
                        !CheckNoConflict(WriteSet.ItemWriteSet, _reads.ItemReadsTimeStamp, _tempState.ItemStore) ||
                        !CheckNoConflict(WriteSet.StockWriteSet, _reads.StockReadsTimeStamp, _tempState.StockStore) ||
                        !CheckNoConflict(WriteSet.HistoryWriteSet, _reads.HistoryReadsTimeStamp, _tempState.HistoryStore) ||
                        !CheckNoConflict(WriteSet.NewOrderWriteSet, _reads.NewOrderReadsTimeStamp, _tempState.NewOrderStore) ||
                        !CheckNoConflict(WriteSet.OrderWriteSet, _reads.OrderReadsTimeStamp, _tempState.OrderStore) ||
                        !CheckNoConflict(WriteSet.OrderLineWriteSet, _reads.OrderLineReadsTimeStamp, _tempState.OrderLineStore))
            {
                Status = TransactionStatus.Failed;
                throw new TransactionFailedException($"Transaction {Tid} aborted due to write conflict");
            }

            var curTime = _timer.GetUtcNow().Ticks;
            SaveToTempState(WriteSet.YCSBWriteSet, _tempState.YCSBStore, curTime);
            SaveToTempState(WriteSet.WarehouseWriteSet, _tempState.WarehouseStore, curTime);
            SaveToTempState(WriteSet.DistrictWriteSet, _tempState.DistrictStore, curTime);
            SaveToTempState(WriteSet.CustomerWriteSet, _tempState.CustomerStore, curTime);
            SaveToTempState(WriteSet.ItemWriteSet, _tempState.ItemStore, curTime);
            SaveToTempState(WriteSet.StockWriteSet, _tempState.StockStore, curTime);
            SaveToTempState(WriteSet.HistoryWriteSet, _tempState.HistoryStore, curTime);
            SaveToTempState(WriteSet.NewOrderWriteSet, _tempState.NewOrderStore, curTime);
            SaveToTempState(WriteSet.OrderWriteSet, _tempState.OrderStore, curTime);
            SaveToTempState(WriteSet.OrderLineWriteSet, _tempState.OrderLineStore, curTime);

        }
        finally
        {
            _rwLock.ExitWriteLock();
        }

        // Set the read and write keys
        ReadKeys = _reads.ReadsTimeStampToReadSet();

        Status = TransactionStatus.LocalCompleted;
    }




    private bool CheckNoConflict<TKey, TValue>(Dictionary<TKey, TValue> writes,
    Dictionary<TKey, long> reads,
    Dictionary<TKey, TempStorage.TempEntry<TValue>> tempState)
    {
        foreach (var (key, _) in writes)
        {
            if (reads.TryGetValue(key, out long readTimestamp))
            {
                long lastUpdateTimestamp = 0;
                bool hasTempEntry = false;
                if (tempState.TryGetValue(key, out var entry))
                {
                    lastUpdateTimestamp = entry.Ts;
                    hasTempEntry = true;
                }


                if (lastUpdateTimestamp > readTimestamp)
                {
                    // Status = TransactionStatus.Failed;
                    // throw new TransactionFailedException($"Transaction {Tid} aborted due to write conflict on key '{key}'.");
                    return false;
                }

                if (hasTempEntry)
                {
                    PrevTx.Add(entry.Tid);
                }
            }
        }

        return true;
    }


    private void Put<TKey, TValue>(Dictionary<TKey, TValue> writeSet, TKey key, TValue value)
    {
        if (Status != TransactionStatus.Active)
        {
            throw new InvalidOperationException($"Transaction {Tid} is not active. Cannot set value for key '{key}'.");
        }

        writeSet[key] = value;
    }

    private bool Get<TKey, TValue>(TKey key,
    out TValue value,
    Dictionary<TKey, TValue> writeSet,
    Database database,
    Dictionary<TKey, TempStorage.TempEntry<TValue>> tempState,
    Dictionary<TKey, long> readsTs,
    Dictionary<TKey, int> keyAccessedFromSnapshot)
    {

        if (Status != TransactionStatus.Active)
        {
            throw new InvalidOperationException($"Transaction {Tid} is not active. Cannot get value for key '{key}'.");
        }

        // check if the key is already written in this transaction
        if (writeSet.TryGetValue(key, out value))
        {
            return true;
        }
        else
        {
            _rwLock.EnterReadLock();
            try
            {
                // check if is in the temporary state first
                if (tempState.TryGetValue(key, out var entry))
                {
                    value = entry.Value;
                    readsTs[key] = entry.Ts;
                    PrevTx.Add(entry.Tid);
                }
                // get a copy from the persistent database
                else if (_persistDb.Get(database, key, out var p1))
                {
                    PersistEntry<TValue> temp = (PersistEntry<TValue>)p1;
                    value = temp.Value;
                    // if its read from the persistent database, then it is not updated in the current epoch
                    readsTs[key] = 0;
                    // then we also need to track which snapshot it was read from
                    keyAccessedFromSnapshot[key] = temp.Cid;
                }
                else
                {
                    value = default;
                    return false;
                }

                return true;
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
    }

    private void SaveToTempState<TKey, TValue>(Dictionary<TKey, TValue> writeSet,
        Dictionary<TKey, TempStorage.TempEntry<TValue>> tempState,
        long curTime)
    {
        foreach (var (key, value) in writeSet)
        {
            tempState[key] = new TempStorage.TempEntry<TValue>(value, curTime, Tid);
        }
    }

    // YCSB Ops
    public override void KeySet(int shard, string key, string value)
    {
        Put<(int, string), string>(WriteSet.YCSBWriteSet, (shard, key), value);
    }

    public override bool KeyGet(int shard, string key, out string value)
    {
        return Get<(int, string), string>((shard, key), out value,
            WriteSet.YCSBWriteSet,
            Database.YCSB,
            _tempState.YCSBStore,
            _reads.YCSBReadsTimeStamp,
            keyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot);
    }

    // TPC-C
    public override void PutOrder(Order order)
    {
        var key = (order.O_W_ID, order.O_D_ID, order.O_ID);

        Put(WriteSet.OrderWriteSet, key, order);
    }

    public override void PutStock(Stock stock)
    {
        var key = (stock.S_W_ID, stock.S_I_ID);

        Put(WriteSet.StockWriteSet, key, stock);
    }

    public override void PutOrderLine(OrderLine orderLine)
    {
        var key = (orderLine.OL_W_ID, orderLine.OL_D_ID, orderLine.OL_O_ID, orderLine.OL_NUMBER);

        Put(WriteSet.OrderLineWriteSet, key, orderLine);
    }

    public override void PutCustomer(Customer customer)
    {
        var key = (customer.C_W_ID, customer.C_D_ID, customer.C_ID);

        Put(WriteSet.CustomerWriteSet, key, customer);
    }

    public override void PutItem(Item item)
    {
        Put(WriteSet.ItemWriteSet, item.I_ID, item);
    }

    public override void PutWarehouse(Warehouse warehouse)
    {
        Put(WriteSet.WarehouseWriteSet, warehouse.W_ID, warehouse);
    }

    public override void PutDistrict(District district)
    {
        var key = (district.D_W_ID, district.D_ID);

        Put(WriteSet.DistrictWriteSet, key, district);
    }

    public override void PutHistory(History history)
    {
        var key = (history.H_C_ID, history.H_DATE);

        Put(WriteSet.HistoryWriteSet, key, history);
    }

    public override void PutNewOrder(NewOrder newOrder)
    {
        var key = (newOrder.NO_W_ID, newOrder.NO_D_ID, newOrder.NO_O_ID);

        Put(WriteSet.NewOrderWriteSet, key, newOrder);
    }

    public override bool GetWarehouse(long warehouseId, out Warehouse warehouse)
    {
        return Get(warehouseId, out warehouse,
            WriteSet.WarehouseWriteSet,
            Database.Warehouse,
        _tempState.WarehouseStore,
            _reads.WarehouseReadsTimeStamp,
            keyAccessedFromSnapshot.WarehouseReadsFromSnapshot);
    }

    public override bool GetDistrict(long warehouseId, long districtId, out District district)
    {
        var key = (warehouseId, districtId);

        return Get(key, out district,
            WriteSet.DistrictWriteSet,
            Database.District,
        _tempState.DistrictStore,
            _reads.DistrictReadsTimeStamp,
            keyAccessedFromSnapshot.DistrictReadsFromSnapshot);
    }

    public override bool GetCustomer(long warehouseId, long districtId, long customerId, out Customer customer)
    {
        var key = (warehouseId, districtId, customerId);

        return GetCustomerByID(key, out customer);

    }

    private bool GetCustomerByID((long, long, long) customerIdStr, out Customer customer)
    {
        return Get(customerIdStr, out customer,
            WriteSet.CustomerWriteSet,
            Database.Customer,
        _tempState.CustomerStore,
            _reads.CustomerReadsTimeStamp,
            keyAccessedFromSnapshot.CustomerReadsFromSnapshot);
    }

    public override bool GetCustomerByLastName(int warehouseId, int districtId, string lastName, out Customer customer)
    {
        StringBuilder sb = new();
        sb.Append(warehouseId);
        sb.Append('-');
        sb.Append(districtId);
        sb.Append('-');
        sb.Append(lastName);
        var customerKey = sb.ToString();

        if (!_persistDb.CustomerIndexByLastName.TryGetValue(customerKey, out var customerList) || customerList.Count == 0)
        {
            customer = null;
            return false;
        }

        if (customerList.Count == 1)
        {
            GetCustomerByID(customerList.Last(), out customer);
            return true;
        }
        else
        {
            List<Customer> customers = [];
            foreach (var id in customerList)
            {
                if (GetCustomerByID(id, out var cust))
                {
                    customers.Add(cust);
                }
            }

            // Sort the customers by CID 
            // Select the customer with the middle C_ID
            customers.Sort((c1, c2) => c1.C_ID.CompareTo(c2.C_ID));
            int middleIndex = customers.Count / 2;
            if (customers.Count % 2 == 0)
            {
                middleIndex--;
            }
            customer = customers[middleIndex];
            return true;
        }
    }


    public override bool GetItem(long itemId, out Item item)
    {
        return Get(itemId, out item,
            WriteSet.ItemWriteSet,
            Database.Item,
        _tempState.ItemStore,
            _reads.ItemReadsTimeStamp,
            keyAccessedFromSnapshot.ItemReadsFromSnapshot);
    }

    public override bool GetStock(long warehouseId, long itemId, out Stock stock)
    {
        var key = (warehouseId, itemId);

        return Get(key, out stock,
            WriteSet.StockWriteSet,
            Database.Stock,
        _tempState.StockStore,
            _reads.StockReadsTimeStamp,
            keyAccessedFromSnapshot.StockReadsFromSnapshot);
    }

    public override bool GetHistory(long customerId, long timestamp, out History history)
    {
        var key = (customerId, timestamp);

        return Get(key, out history,
            WriteSet.HistoryWriteSet,
            Database.History,
        _tempState.HistoryStore,
            _reads.HistoryReadsTimeStamp,
            keyAccessedFromSnapshot.HistoryReadsFromSnapshot);
    }

    public override bool GetNewOrder(long warehouseId, long districtId, long orderId, out NewOrder newOrder)
    {
        var key = (warehouseId, districtId, orderId);

        return Get(key, out newOrder,
            WriteSet.NewOrderWriteSet,
            Database.NewOrder,
        _tempState.NewOrderStore,
            _reads.NewOrderReadsTimeStamp,
            keyAccessedFromSnapshot.NewOrderReadsFromSnapshot);
    }

    public override bool GetOrder(long warehouseId, long districtId, long orderId, out Order order)
    {
        var key = (warehouseId, districtId, orderId);

        return Get(key, out order,
            WriteSet.OrderWriteSet,
            Database.Order,
        _tempState.OrderStore,
            _reads.OrderReadsTimeStamp,
            keyAccessedFromSnapshot.OrderReadsFromSnapshot);
    }

    public override bool GetOrderLine(long warehouseId, long districtId, long orderId, long orderLineNumber, out OrderLine orderLine)
    {
        var key = (warehouseId, districtId, orderId, orderLineNumber);

        return Get(key, out orderLine,
            WriteSet.OrderLineWriteSet,
            Database.OrderLine,
        _tempState.OrderLineStore,
            _reads.OrderLineReadsTimeStamp,
            keyAccessedFromSnapshot.OrderLineReadsFromSnapshot);
    }

}
