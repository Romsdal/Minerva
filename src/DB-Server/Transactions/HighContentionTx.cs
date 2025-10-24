using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;

namespace Minerva.DB_Server.Transactions;

public class HighContentionTx : MinervaTx
{
    new public WriteSetStore WriteSet = new();
    new public ReadSetStore ReadKeys = new();

    private PersistentStorage _persistDb;

    /// Transaction that does not execute but only collects read and write sets.
    public HighContentionTx(int tid, PersistentStorage persistDb) : base(tid, persistDb, null, null, null)
    {
        _persistDb = persistDb;
    }

    public override void Abort()
    {
        throw new InvalidOperationException($"Cannot abort a HighContentionTx transaction {Tid} explicitly.");
    }

    public override void Begin()
    {
        Status = TransactionStatus.DidNotExecute;
    }

    public override void Complete()
    {

    }

    private void Put<TKey, TValue>(Dictionary<TKey, TValue> writeSet, TKey key)
    {
        writeSet[key] = default;
    }

    private bool Get<TKey, TValue>(TKey key,
    out TValue value,
    List<TKey> ReadSet,
    Database database
    )
    {
        ReadSet.Add(key);
        if (_persistDb.Get(database, key, out var p1))
        {
            PersistEntry<TValue> temp = (PersistEntry<TValue>)p1;
            value = temp.Value;
        }
        else
        {
            value = default;
            return false;
        }

        return true;

    }

    // YCSB Ops
    public override void KeySet(int shard, string key, string value)
    {
        Put<(int, string), string>(WriteSet.YCSBWriteSet, (shard, key));
    }

    public override bool KeyGet(int shard, string key, out string value)
    {
        return Get<(int, string), string>((shard, key), out value,
            ReadKeys.YCSBReadKeys,
            Database.YCSB);
    }

    // TPC-C
    public override void PutOrder(Order order)
    {
        var key = (order.O_W_ID, order.O_D_ID, order.O_ID);

        Put(WriteSet.OrderWriteSet, key);
    }

    public override void PutStock(Stock stock)
    {
        var key = (stock.S_W_ID, stock.S_I_ID);

        Put(WriteSet.StockWriteSet, key);
    }

    public override void PutOrderLine(OrderLine orderLine)
    {
        var key = (orderLine.OL_W_ID, orderLine.OL_D_ID, orderLine.OL_O_ID, orderLine.OL_NUMBER);

        Put(WriteSet.OrderLineWriteSet, key);
    }

    public override void PutCustomer(Customer customer)
    {
        var key = (customer.C_W_ID, customer.C_D_ID, customer.C_ID);

        Put(WriteSet.CustomerWriteSet, key);
    }

    public override void PutItem(Item item)
    {
        Put(WriteSet.ItemWriteSet, item.I_ID);
    }

    public override void PutWarehouse(Warehouse warehouse)
    {
        Put(WriteSet.WarehouseWriteSet, warehouse.W_ID);
    }

    public override void PutDistrict(District district)
    {
        var key = (district.D_W_ID, district.D_ID);

        Put(WriteSet.DistrictWriteSet, key);
    }

    public override void PutHistory(History history)
    {
        var key = (history.H_C_ID, history.H_DATE);

        Put(WriteSet.HistoryWriteSet, key);
    }

    public override void PutNewOrder(NewOrder newOrder)
    {
        var key = (newOrder.NO_W_ID, newOrder.NO_D_ID, newOrder.NO_O_ID);

        Put(WriteSet.NewOrderWriteSet, key);
    }

    public override bool GetWarehouse(long warehouseId, out Warehouse warehouse)
    {
        return Get(warehouseId, out warehouse,
            ReadKeys.WarehouseReadKeys,
            Database.Warehouse);
    }

    public override bool GetDistrict(long warehouseId, long districtId, out District district)
    {
        var key = (warehouseId, districtId);

        return Get(key, out district,
            ReadKeys.DistrictReadKeys,
            Database.District);
    }

    public override bool GetCustomer(long warehouseId, long districtId, long customerId, out Customer customer)
    {
        var key = (warehouseId, districtId, customerId);

        return GetCustomerByID(key, out customer);

    }

    private bool GetCustomerByID((long, long, long) customerIdStr, out Customer customer)
    {
        return Get(customerIdStr, out customer,
            ReadKeys.CustomerReadKeys,
            Database.Customer);
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
            ReadKeys.ItemReadKeys,
            Database.Item);
    }

    public override bool GetStock(long warehouseId, long itemId, out Stock stock)
    {
        var key = (warehouseId, itemId);

        return Get(key, out stock,
            ReadKeys.StockReadKeys,
            Database.Stock);
    }

    public override bool GetHistory(long customerId, long timestamp, out History history)
    {
        var key = (customerId, timestamp);

        return Get(key, out history,
            ReadKeys.HistoryReadKeys,
            Database.History);
    }

    public override bool GetNewOrder(long warehouseId, long districtId, long orderId, out NewOrder newOrder)
    {
        var key = (warehouseId, districtId, orderId);

        return Get(key, out newOrder,
            ReadKeys.NewOrderReadKeys,
            Database.NewOrder);
    }

    public override bool GetOrder(long warehouseId, long districtId, long orderId, out Order order)
    {
        var key = (warehouseId, districtId, orderId);

        return Get(key, out order,
            ReadKeys.OrderReadKeys,
            Database.Order);
    }

    public override bool GetOrderLine(long warehouseId, long districtId, long orderId, long orderLineNumber, out OrderLine orderLine)
    {
        var key = (warehouseId, districtId, orderId, orderLineNumber);

        return Get(key, out orderLine,
            ReadKeys.OrderLineReadKeys,
            Database.OrderLine);
    }

}