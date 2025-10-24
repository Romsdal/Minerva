using System;
using System.Collections.Generic;
using System.Linq;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.Benchmarks;

/// <summary>
/// Query format "4:warehouseId;districtId;customerId;itemid,supplyWarehouseId,quantity;itemid,supplyWarehouseId,quantity..."
/// </summary>
/// <param name="query"></param>
/// <param name="argsIndex"></param>
/// <param name="transactionManager"></param>
/// <param name="get"></param>
/// <param name="put"></param> <summary>
public class TxnNewOrder(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{
    private Transaction _tx;

    public override Transaction Execute()
    {
        _tx = _transactionManager.CreateTransaction();
        _tx.Begin();

        var req = _query.Tpccno;
        int warehouseId = req.W_ID;
        int districtId = req.D_ID;
        int customerId = req.C_ID;
        List<(int itemId, int supplyWarehouseId, int quantity)> items = [];

        foreach (var item in req.Items)
        {
            int itemId = item.I_ID;
            int supplyWarehouseId = item.W_ID;
            int quantity = item.Q;
            items.Add((itemId, supplyWarehouseId, quantity));
        }
        ProcessNewOrder(warehouseId, districtId, customerId, items);


        _tx.Complete();
        return _tx;
    }

    public void ProcessNewOrder(int warehouseId, int districtId, int customerId, List<(int itemId, int supplyWarehouseId, int quantity)> items)
    {

        _tx.GetWarehouse(warehouseId, out var warehouse);
        _tx.GetDistrict(warehouseId, districtId, out var district);
        _tx.GetCustomer(warehouseId, districtId, customerId, out var customer);

        long nextOrderId = district.D_NEXT_O_ID;

        District districtPatch = new()
        {
            Is_Patch = true,
            D_ID = district.D_ID,
            D_W_ID = district.D_W_ID,
            D_NEXT_O_ID = district.D_NEXT_O_ID + 1,
        };
        _tx.PutDistrict(districtPatch);

        bool allLocal = items.All(item => item.supplyWarehouseId == warehouseId);

        var order = new Order
        {
            O_ID = nextOrderId,
            O_D_ID = districtId,
            O_W_ID = warehouseId,
            O_C_ID = customerId,
            O_ENTRY_D = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            O_OL_CNT = items.Count,
            O_ALL_LOCAL = allLocal
        };

        _tx.PutOrder(order);

        var newOrder = new NewOrder
        {
            NO_O_ID = nextOrderId,
            NO_D_ID = districtId,
            NO_W_ID = warehouseId
        };
        _tx.PutNewOrder(newOrder);


        double totalAmount = 0;

        for (int i = 0; i < items.Count; i++)
        {
            var (itemId, supplyWarehouseId, quantity) = items[i];


            if (!_tx.GetItem(itemId, out var item))
            {
                _tx.Abort();
            }

            _tx.GetStock(warehouseId, itemId, out var stock);
            Stock stockPatch = new()
            {
                S_W_ID = stock.S_W_ID,
                S_I_ID = stock.S_I_ID,
                Is_Patch = true,
            };

            if (stock.S_QUANTITY - quantity < 10)
            {
                //update stock quantity to reflect the shortage
                //writeline how many items were added to the stock
                //Console.WriteLine($"Stock shortage for item {itemId} in warehouse {supplyWarehouseId}. Number: {stock.S_QUANTITY } Adding 91 items to stock.");
                stockPatch.S_QUANTITY = stock.S_QUANTITY - quantity + 91;

            }
            else
            {
                stockPatch.S_QUANTITY = stock.S_QUANTITY - quantity;
            }

            stockPatch.S_YTD = stock.S_YTD + quantity;
            stockPatch.S_ORDER_CNT = stock.S_ORDER_CNT + 1;
            stockPatch.S_REMOTE_CNT = stock.S_REMOTE_CNT + (supplyWarehouseId != warehouseId ? 1 : 0);

            _tx.PutStock(stockPatch);

            double itemAmount = item.I_PRICE * quantity;
            totalAmount += itemAmount;

            var orderLine = new OrderLine
            {
                OL_O_ID = nextOrderId,
                OL_D_ID = districtId,
                OL_W_ID = warehouseId,
                OL_NUMBER = i + 1,
                OL_I_ID = itemId,
                OL_SUPPLY_W_ID = supplyWarehouseId,
                OL_QUANTITY = quantity,
                OL_AMOUNT = itemAmount,
                OL_DELIVERY_D = 0, // TPC-C: NULL for new orders
                OL_DIST_INFO = "OL Info" // temporary

            };
            _tx.PutOrderLine(orderLine);
        }

        double finalAmount = totalAmount * (1 + warehouse.W_TAX + district.D_TAX) * (1 - customer.C_DISCOUNT);
        // In a real scenario, you'd likely save this finalAmount somewhere.
        // For now, we'll just ensure the logic is in place.
    }
}
