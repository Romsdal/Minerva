using System;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Transactions;


public enum TransactionStatus
{
    Inactive,
    Active,
    Failed, // Failed due to error or OCC abort
    OCCRetry,
    Aborted, // Aborted due to consistency check
    LocalCompleted,
    DidNotExecute, // used in high contention mode
}

public interface ITransactionManager
{
    Transaction CreateTransaction();
}

public interface ITransaction
{
    public void Abort();
    public void Begin();
    public void Complete();

    public void KeySet(int shard, string key, string value);
    public bool KeyGet(int shard, string key, out string value);

    // TPC-C Put operations
    public void PutOrder(Order order);
    public void PutStock(Stock stock);
    public void PutOrderLine(OrderLine orderLine);
    public void PutCustomer(Customer customer);
    public void PutItem(Item item);
    public void PutWarehouse(Warehouse warehouse);
    public void PutDistrict(District district);
    public void PutHistory(History history);
    public void PutNewOrder(NewOrder newOrder);

    // TPC-C Get operations
    public bool GetWarehouse(long warehouseId, out Warehouse warehouse);
    public bool GetDistrict(long warehouseId, long districtId, out District district);
    public bool GetCustomer(long warehouseId, long districtId, long customerId, out Customer customer);
    public bool GetCustomerByLastName(int warehouseId, int districtId, string lastName, out Customer customer);
    public bool GetItem(long itemId, out Item item);
    public bool GetStock(long warehouseId, long itemId, out Stock stock);
    public bool GetHistory(long customerId, long timestamp, out History history);
    public bool GetNewOrder(long warehouseId, long districtId, long orderId, out NewOrder newOrder);
    public bool GetOrder(long warehouseId, long districtId, long orderId, out Order order);
    public bool GetOrderLine(long warehouseId, long districtId, long orderId, long orderLineNumber, out OrderLine orderLine);
}

public class TransactionFailedException : Exception
{
    public TransactionFailedException() : base() { }
    public TransactionFailedException(string message) : base(message) { }
    public TransactionFailedException(string message, Exception innerException) : base(message, innerException) { }
}

public class TransactionAbortedException : Exception
{
    public TransactionAbortedException() : base() { }
    public TransactionAbortedException(string message) : base(message) { }
    public TransactionAbortedException(string message, Exception innerException) : base(message, innerException) { }
}
