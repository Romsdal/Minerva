using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;
using static Minerva.DB_Server.Storage.PersistentStorage;

namespace Minerva.DB_Server.Transactions;

public class DeterministicTx : Transaction
{
    private readonly PersistentStorage _persistDb;
    private readonly int _currCid;

    private readonly WriteSetStore WriteSet = new();


    public DeterministicTx(int tid, PersistentStorage persistDb, int cid) : base(tid)
    {
        _persistDb = persistDb;
        _currCid = cid;
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
        lock (_persistDb)
        {
            TxStateHelpers.ApplyWriteSetToPersistentDB(WriteSet, _persistDb, _currCid);
        }
    }

    public override void KeySet(int shard, string key, string value)
    {
        Put<(int, string), string>(WriteSet.YCSBWriteSet, (shard, key), value);
    }

    public override bool KeyGet(int shard, string key, out string value)
    {
        return Get(WriteSet.YCSBWriteSet, Database.YCSB, (shard, key), out value);

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
        return Get(WriteSet.WarehouseWriteSet, Database.Warehouse, warehouseId, out warehouse);

    }

    public override bool GetDistrict(long warehouseId, long districtId, out District district)
    {

        var key = (warehouseId, districtId);

        return Get(WriteSet.DistrictWriteSet, Database.District, key, out district);

    }

    public override bool GetCustomer(long warehouseId, long districtId, long customerId, out Customer customer)
    {
        var key = (warehouseId, districtId, customerId);

        return GetCustomerByID(key, out customer);
    }

    private bool GetCustomerByID((long, long, long) customerId, out Customer customer)
    {
        return Get(WriteSet.CustomerWriteSet, Database.Customer, customerId, out customer);
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
        return Get(WriteSet.ItemWriteSet, Database.Item, itemId, out item);
    }

    public override bool GetStock(long warehouseId, long itemId, out Stock stock)
    {
        var key = (warehouseId, itemId);

        return Get(WriteSet.StockWriteSet, Database.Stock, key, out stock);
    }

    public override bool GetHistory(long customerId, long timestamp, out History history)
    {
        var key = (customerId, timestamp);
        return Get(WriteSet.HistoryWriteSet, Database.History, key, out history);

    }

    public override bool GetNewOrder(long warehouseId, long districtId, long orderId, out NewOrder newOrder)
    {
        var key = (warehouseId, districtId, orderId);
        return Get(WriteSet.NewOrderWriteSet, Database.NewOrder, key, out newOrder);

    }

    public override bool GetOrder(long warehouseId, long districtId, long orderId, out Order order)
    {

        var key = (warehouseId, districtId, orderId);
        return Get(WriteSet.OrderWriteSet, Database.Order, key, out order);

    }

    public override bool GetOrderLine(long warehouseId, long districtId, long orderId, long orderLineNumber, out OrderLine orderLine)
    {
        var key = (warehouseId, districtId, orderId, orderLineNumber);

        return Get(WriteSet.OrderLineWriteSet, Database.OrderLine, key, out orderLine);
    }


    private static void Put<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, TValue value)
    {
        dictionary[key] = value;
    }
    
    private bool Get<TKey, TValue>(Dictionary<TKey, TValue> writeSet, Database database, TKey key, out TValue value)
    {
        
        if (writeSet.TryGetValue(key, out value))
        {
            return true;
        }
        else if (_persistDb.Get<TKey>(database, key, out object entry))
        {
            value = ((PersistEntry<TValue>)entry).Value;
            return true;
        }
        else
        {
            value = default;
            return false;
        }
    }


}