using System;
using System.Collections.Generic;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Transactions;


public abstract class Transaction(int tid) : ITransaction
{
    public readonly int Tid = tid;
    public TransactionStatus Status { get; set; } = TransactionStatus.Inactive;
    public string Result { get; set; } = string.Empty;

    public abstract void Abort();
    public abstract void Begin();
    public abstract void Complete();

    public abstract void KeySet(int shard, string key, string value);
    public abstract bool KeyGet(int shard, string key, out string value);

    // TPC-C Put operations
    public abstract void PutOrder(Order order);
    public abstract void PutStock(Stock stock);
    public abstract void PutOrderLine(OrderLine orderLine);
    public abstract void PutCustomer(Customer customer);
    public abstract void PutItem(Item item);
    public abstract void PutWarehouse(Warehouse warehouse);
    public abstract void PutDistrict(District district);
    public abstract void PutHistory(History history);
    public abstract void PutNewOrder(NewOrder newOrder);

    // TPC-C Get operations
    public abstract bool GetWarehouse(long warehouseId, out Warehouse warehouse);
    public abstract bool GetDistrict(long warehouseId, long districtId, out District district);
    public abstract bool GetCustomer(long warehouseId, long districtId, long customerId, out Customer customer);
    public abstract bool GetCustomerByLastName(int warehouseId, int districtId, string lastName, out Customer customer);
    public abstract bool GetItem(long itemId, out Item item);
    public abstract bool GetStock(long warehouseId, long itemId, out Stock stock);
    public abstract bool GetHistory(long customerId, long timestamp, out History history);
    public abstract bool GetNewOrder(long warehouseId, long districtId, long orderId, out NewOrder newOrder);
    public abstract bool GetOrder(long warehouseId, long districtId, long orderId, out Order order);
    public abstract bool GetOrderLine(long warehouseId, long districtId, long orderId, long orderLineNumber, out OrderLine orderLine);
}





