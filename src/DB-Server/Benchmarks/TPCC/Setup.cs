using System;
using System.Collections.Generic;
using System.Linq;
using Minerva.DB_Server;
using Minerva.DB_Server.Benchmarks;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.Benchmarks;



/// Note: It is impossible for load tx to trigger re-execution - therefore random is fine here

/// <summary>
/// Query Format "9:I_ID;"
/// </summary>
/// <param name="query"></param>
/// <param name="argsIndex"></param>
/// <param name="transactionManager"></param> <summary>

public class LoadItems(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{
    public override Transaction Execute()
    {
        var _random = new TPCCRandom();

        var _tx = _transactionManager.CreateTransaction();

        _tx.Begin();



        for (int i = _query.Tpccli.I * 1000; i < (_query.Tpccli.I + 1) * 1000; i++)
        {
            int iid = i + 1;

            var item = new Item
            {
                I_ID = iid,
                I_NAME = _random.GenerateItemName(),
                I_PRICE = (double)_random.GenerateItemPrice(),
                I_DATA = _random.GenerateItemData(),
                I_IM_ID = _random.RandInt(1, 10000)
            };
            _tx.PutItem(item);
        }

        _tx.Complete();

        return _tx;
    }
}


/// <summary>
/// Query Format "10:w_ID;"
/// </summary>
/// <param name="query"></param>
/// <param name="argsIndex"></param>
/// <param name="transactionManager"></param> <summary>
public class LoadWarehouses(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{

    public override Transaction Execute()
    {
        var _random = new TPCCRandom();
        var _tx = _transactionManager.CreateTransaction();

        _tx.Begin();

        var warehouse = new Warehouse
        {
            W_ID = _query.Tpcclw.W,
            W_NAME = _random.GenerateWarehouseName(),
            W_STREET_1 = _random.GenerateCustomerStreet(),
            W_STREET_2 = _random.GenerateCustomerStreet(),
            W_CITY = _random.GenerateCustomerCity(),
            W_STATE = _random.GenerateState(),
            W_ZIP = _random.GenerateZipCode(),
            W_TAX = (double)_random.GenerateTaxRate(),
            W_YTD = 300000.00
        };
        _tx.PutWarehouse(warehouse);

        _tx.Complete();

        return _tx;
    }
}

/// <summary>
/// Query Format "11:d_ID;W_ID"
/// </summary>
/// <param name="args"></param>
/// <param name="transactionManager"></param> <summary>
/// 
/// </summary>
/// <typeparam name="byte[]"></typeparam>
public class LoadDistricts(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{

    public override Transaction Execute()
    {
        var _random = new TPCCRandom();
        var _tx = _transactionManager.CreateTransaction();

        _tx.Begin();

    
        var district = new District
        {
            D_ID = _query.Tpccld.D,
            D_W_ID = _query.Tpccld.WarehouseId,
            D_NAME = _random.GenerateDistrictName(),
            D_STREET_1 = _random.GenerateCustomerStreet(),
            D_STREET_2 = _random.GenerateCustomerStreet(),
            D_CITY = _random.GenerateCustomerCity(),
            D_STATE = _random.GenerateState(),
            D_ZIP = _random.GenerateZipCode(),
            D_TAX = (double)_random.GenerateTaxRate(),
            D_YTD = 30000.00,
            D_NEXT_O_ID = 3001
        };
        _tx.PutDistrict(district);

        _tx.Complete();

        return _tx;
    }
}


/// <summary>
/// Query Format "12:C_ID;C_D_ID;C_W_ID
/// </summary>
/// <param name="args"></param>
/// <param name="transactionManager"></param> <summary>
/// 
/// </summary>
/// <typeparam name="byte[]"></typeparam>
public class LoadCustomers(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{

    public override Transaction Execute()
    {
        var _random = new TPCCRandom();
        var _tx = _transactionManager.CreateTransaction();

        _tx.Begin();

        for (int c = _query.Tpcclc.C * 100; c < (_query.Tpcclc.C + 1) * 100; c++)
        {
            int cid = c + 1;

            var customer = new Customer
            {
                C_ID = cid,
                C_D_ID = _query.Tpcclc.DistrictID,
                C_W_ID = _query.Tpcclc.WarehouseId,
                C_FIRST = _random.GenerateCustomerFirstName(),
                C_MIDDLE = "OE", // TPC-C specification
                C_LAST = (cid <= 1000) ? _random.LastName((int)cid - 1) : _random.GetNonUniformCustomerLastNameLoad(),
                C_STREET_1 = _random.GenerateCustomerStreet(),
                C_STREET_2 = _random.GenerateCustomerStreet(),
                C_CITY = _random.GenerateCustomerCity(),
                C_STATE = _random.GenerateState(),
                C_ZIP = _random.GenerateZipCode(),
                C_PHONE = _random.GeneratePhoneNumber(),
                C_SINCE = DateTimeOffset.Now.ToUnixTimeSeconds(),
                C_CREDIT = _random.GenerateCreditStatus(),
                C_CREDIT_LIM = _random.GenerateCustomerCreditLimit(),
                C_DISCOUNT = (double)_random.GenerateDiscountRate(),
                C_BALANCE = (double)_random.GenerateCustomerBalance(),
                C_YTD_PAYMENT = (double)_random.GenerateCustomerYtdPayment(),
                C_PAYMENT_CNT = _random.GenerateCustomerPaymentCount(),
                C_DELIVERY_CNT = _random.GenerateCustomerDeliveryCount(),
                C_DATA = _random.GenerateCustomerData()
            };
            // Console.WriteLine($"Loading customer {customer.C_FIRST} {customer.C_LAST} for district {districtId} in warehouse {warehouseId}");
            _tx.PutCustomer(customer);
        }


        _tx.Complete();

        return _tx;
    }
}


/// <summary>
/// Query Format "13:S_I_ID;S_W_ID
/// </summary>
/// <param name="args"></param>
/// <param name="transactionManager"></param> <summary>
/// 
/// </summary>
/// <typeparam name="byte[]"></typeparam>
public class LoadStock(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{

    public override Transaction Execute()
    {
        var _random = new TPCCRandom();
        var _tx = _transactionManager.CreateTransaction();

        _tx.Begin();

        for (int i = _query.Tpccls.I * 1000; i < (_query.Tpccls.I + 1) * 1000; i++)
        {
            var sid = i + 1; // TPC-C item IDs are 1-indexed

            var stock = new Stock
            {
                S_I_ID = sid,
                S_W_ID = _query.Tpccls.WarehouseId,
                S_QUANTITY = _random.GenerateStockQuantity(),
                S_DIST_01 = _random.GenerateDistInfo(),
                S_DIST_02 = _random.GenerateDistInfo(),
                S_DIST_03 = _random.GenerateDistInfo(),
                S_DIST_04 = _random.GenerateDistInfo(),
                S_DIST_05 = _random.GenerateDistInfo(),
                S_DIST_06 = _random.GenerateDistInfo(),
                S_DIST_07 = _random.GenerateDistInfo(),
                S_DIST_08 = _random.GenerateDistInfo(),
                S_DIST_09 = _random.GenerateDistInfo(),
                S_DIST_10 = _random.GenerateDistInfo(),
                S_YTD = 0,
                S_ORDER_CNT = 0,
                S_REMOTE_CNT = 0,
                S_DATA = _random.GenerateStockDataWithOriginal(_random.RandInt(26, 50))
            };
            _tx.PutStock(stock);
        }


        _tx.Complete();

        return _tx;
    }

}

/// <summary>
/// Query Format "14:O_ID;customerId;O_D_ID;O_W_ID
/// </summary>
/// <param name="args"></param>
/// <param name="transactionManager"></param> <summary>
/// 
/// </summary>
/// <typeparam name="byte[]"></typeparam>
public class LoadInitialOrders(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{

    public override Transaction Execute()
    {
        var _random = new TPCCRandom();
        var _tx = _transactionManager.CreateTransaction();

        _tx.Begin();


        int d = _query.Tpcclio.D;
        int w = _query.Tpcclio.W;


        var customerIds = Enumerable.Range(1, 3000).OrderBy(x => _random.RandInt(1, 999999)).ToList();

        for (int o = _query.Tpcclio.O * 100; o < (_query.Tpcclio.O + 1) * 100; o++)
        {
            int oid = o + 1;
            var customerId = customerIds[oid - 1];
            var order = new Order
            {
                O_ID = oid,
                O_C_ID = customerId,
                O_D_ID = d,
                O_W_ID = w,
                O_ENTRY_D = DateTimeOffset.Now.AddDays(-_random.RandInt(0, 365)).ToUnixTimeSeconds(),
                O_CARRIER_ID = oid < 2101 ? _random.RandInt(1, 10) : 0, // Orders < 2101 are delivered
                O_OL_CNT = _random.RandInt(5, 15),
                O_ALL_LOCAL = true
            };
            _tx.PutOrder(order);

            // Generate order lines for this order
            for (int ol = 1; ol <= order.O_OL_CNT; ol++)
            {
                var orderLine = new OrderLine
                {
                    OL_O_ID = oid,
                    OL_D_ID = d,
                    OL_W_ID = w,
                    OL_NUMBER = ol,
                    OL_I_ID = _random.RandInt(1, 100000),
                    OL_SUPPLY_W_ID = w,
                    OL_DELIVERY_D = oid < 2101 ? order.O_ENTRY_D : 0, // Delivered if order < 2101
                    OL_QUANTITY = 5,
                    OL_AMOUNT = oid < 2101 ? 0 : (double)_random.GenerateItemPrice(), // Amount 0 for delivered orders
                    OL_DIST_INFO = _random.GenerateDistInfo()
                };
                _tx.PutOrderLine(orderLine);
            }

            // Add to NewOrder table if order >= 2101 (pending delivery)
            if (oid >= 2101)
            {
                var newOrder = new NewOrder
                {
                    NO_O_ID = oid,
                    NO_D_ID = d,
                    NO_W_ID = w
                };
                _tx.PutNewOrder(newOrder);
            }

        }
        _tx.Complete();

        return _tx;
    }
}


/// <summary>
/// Query Format "15:c;d;w
/// </summary>
/// <param name="args"></param>
/// <param name="transactionManager"></param> <summary>
/// 
/// </summary>
/// <typeparam name="byte[]"></typeparam>
public class LoadInitialHistory(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{

    public override Transaction Execute()
    {
        var _random = new TPCCRandom();
        var _tx = _transactionManager.CreateTransaction();

        _tx.Begin();

        for (int c = _query.Tpcclh.C * 100; c < (_query.Tpcclh.C + 1) * 100; c++)
        {
            int cid = c + 1;
            var history = new History
            {
                H_C_ID = cid,
                H_C_D_ID = _query.Tpcclh.D,
                H_C_W_ID = _query.Tpcclh.W,
                H_D_ID = _query.Tpcclh.D,
                H_W_ID = _query.Tpcclh.W,
                H_DATE = DateTimeOffset.Now.AddDays(-_random.RandInt(0, 30)).ToUnixTimeSeconds(),
                H_AMOUNT = 10.00, // Initial payment amount
                H_DATA = $"Initial payment for customer {cid}"
            };

            _tx.PutHistory(history);
        }

        _tx.Complete();

        return _tx;
    }

}




