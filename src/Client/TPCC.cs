
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.Grpc_Client;

public class TPCC : IBenchmarkWorkload
{
    public TPCCConfig Config { get; init; }

    public bool IsLoadComplete { get; private set; } = false;
    private readonly ConcurrentQueue<TxInfo> loadQueryQueue = [];


    public float Progress { get; private set; } = 0;
    private readonly TPCCRandom _rand;

    private int _numWarehouses;

    public TPCC(TPCCConfig config)
    {
        Config = config;
        _rand = new TPCCRandom();
        _numWarehouses = config.NumWarehouse;

        Console.WriteLine($"TPC-C Benchmark initializing with {Config.NumWarehouse} warehouses.");
        LoadItems();
        LoadWarehouses(Config.NumWarehouse);
        LoadInitialOrders(Config.NumWarehouse);
        LoadInitialHistory(Config.NumWarehouse);

        _totalToLoad = loadQueryQueue.Count;
        Console.WriteLine($"TPC-C Benchmark initialization complete.");
    }

    private int _totalToLoad;
    private int loadQueryRan = 0;
    public bool GetNextLoadDataQuery(out TxInfo query)
    {
        Progress = (float)loadQueryRan / _totalToLoad;
        loadQueryRan++;

        if (!loadQueryQueue.TryDequeue(out query))
        {
            IsLoadComplete = true;
            return false;
        }

        if (loadQueryRan % 5000 == 0)
        {
            Thread.Sleep(2000);
        }
        


        return true;
    }

    private void LoadItems()
    {
        for (int i = 0; i < 100; i++)
        {

            var query = new ClientRequest()
            {
                Type = QueryType.Tpccli,
                Tpccli = new TPCCLI()
                {
                    I = i,
                }
            };

            var q = new TxInfo
            {
                Query = query,
                Type = TxType.TPCC_LD
            };
            loadQueryQueue.Enqueue(q);
        }
    }

    private void LoadWarehouses(int _numWarehouses)
    {
        for (int w = 1; w <= _numWarehouses; w++)
        {
            var query = new ClientRequest()
            {
                Type = QueryType.Tpcclw,
                Tpcclw = new TPCCLW()
                {
                    W = w,
                }
            };

            var q = new TxInfo
            {
                Query = query,
                Type = TxType.TPCC_LD
            };
            loadQueryQueue.Enqueue(q);

            LoadDistricts(w);
            LoadStock(w);
        }
    }

    private void LoadDistricts(int warehouseId)
    {
        for (int d = 1; d <= 10; d++)
        {
            var query = new ClientRequest()
            {
                Type = QueryType.Tpccld,
                Tpccld = new TPCCLD()
                {
                    D = d,
                    WarehouseId = warehouseId,
                }
            };

            var q = new TxInfo
            {
                Query = query,
                Type = TxType.TPCC_LD
            };
            loadQueryQueue.Enqueue(q);

            LoadCustomers(warehouseId, d);
        }
    }


    private void LoadCustomers(int warehouseId, int districtId)
    {

        for (int c = 0; c < 30; c++)
        {
            var query = new ClientRequest()
            {
                Type = QueryType.Tpcclc,
                Tpcclc = new TPCCLC()
                {
                    WarehouseId = warehouseId,
                    DistrictID = districtId,
                    C = c
                }
            };

            var q = new TxInfo
            {
                Query = query,
                Type = TxType.TPCC_LD
            };
            loadQueryQueue.Enqueue(q);
        }
        
    }


    private void LoadStock(int warehouseId)
    {
        for (int i = 0; i < 100; i++)
        {
            var query = new ClientRequest()
            {
                Type = QueryType.Tpccls,
                Tpccls = new TPCCLS()
                {
                    I = i,
                    WarehouseId = warehouseId,
                }
            };

            var q = new TxInfo
            {
                Query = query,
                Type = TxType.TPCC_LD
            };
            loadQueryQueue.Enqueue(q);
        }
    }

    private void LoadInitialOrders(int _numWarehouses)
    {
        for (int w = 1; w <= _numWarehouses; w++)
        {
            for (int d = 1; d <= 10; d++)
            {
                for (int o = 0; o < 30; o++)
                {
                    var query = new ClientRequest()
                    {
                        Type = QueryType.Tpcclio,
                        Tpcclio = new TPCCLIO()
                        {
                            D = d,
                            W = w,
                            O = o
                        }
                    };

                    var q = new TxInfo
                    {
                        Query = query,
                        Type = TxType.TPCC_LD
                    };
                    loadQueryQueue.Enqueue(q);
                }
            }
        }
    }


    private void LoadInitialHistory(int _numWarehouses)
    {
        for (int w = 1; w <= _numWarehouses; w++)
        {
            for (int d = 1; d <= 10; d++)
            {

                for (int c = 0; c < 30; c++)
                {
                    var query = new ClientRequest()
                    {
                        Type = QueryType.Tpcclh,
                        Tpcclh = new TPCCLH()
                        {
                            D = d,
                            W = w,
                            C = c
                        }
                    };

                    var q = new TxInfo
                    {
                        Query = query,
                        Type = TxType.TPCC_LD
                    };
                    loadQueryQueue.Enqueue(q);
                }
            }
        }
    }

    public bool GetNextQuery(out TxInfo query)
    {
        var transactionMix = _rand.RandInt(1, 100);

        // TPC-C transaction mix: 45% NewOrder, 43% Payment, 4% Delivery, 4% OrderStatus, 4% StockLevel
        if (transactionMix <= 50)
        {
            query = ExecuteNewOrder();
        }
        else if (transactionMix <= 100)
        {
            query = ExecutePayment();
        }
        // else if (transactionMix <= 92)
        // {
        //     query = ExecuteDelivery();
        // }
        // else if (transactionMix <= 96)
        // {
        //     query = ExecuteOrderStatus();
        // }
        // else if (transactionMix <= 100)
        // {
        //     query = ExecuteStockLevel();
        // }
        else
        {
            query = default;
            return false;
        }

        return true;
    }

    private TxInfo ExecuteNewOrder()
    {

        

            int w_id = _rand.RandInt(1, _numWarehouses);
            int d_id = _rand.RandInt(1, 10);
            int c_id = _rand.GetCustomerId();
            int ol_cnt = _rand.RandInt(5, 15);

            var noItems = new List<NOItem>();
            for (int i = 0; i < ol_cnt; i++)
            {
                int ol_i_id = _rand.GetItemId();
                int ol_supply_w_id = w_id;
                if (_numWarehouses > 1 && _rand.RandInt(1, 100) == 1)
                {
                    do
                    {
                        ol_supply_w_id = _rand.RandInt(1, _numWarehouses);
                    } while (ol_supply_w_id == w_id);
                }
                int ol_quantity = _rand.RandInt(1, 10);

                noItems.Add(new NOItem()
                {
                    I_ID = ol_i_id,
                    W_ID = ol_supply_w_id,
                    Q = ol_quantity
                });
            }

            // TPC-C 2.4.1.5: 1% of transactions should use invalid item for rollback test
            bool useInvalidItem = _rand.RandInt(1, 100) == 1;

            var newOrder = new TPCCNO()
            {
                W_ID = w_id,
                D_ID = d_id,
                C_ID = c_id,
                O_ID = 0, // This might need to be generated or managed differently
                Items = []
            };
            
            newOrder.Items.AddRange(noItems);

        

        var query = new ClientRequest()
        {
            Type = QueryType.Tpccno,
            Tpccno = newOrder
        };

        return new TxInfo
        {
            Query = query,
            Type = TxType.TPCC_NO
        };
    }

    private TxInfo ExecutePayment()
    {
        int w_id = _rand.RandInt(1, _numWarehouses);
        int d_id = _rand.RandInt(1, 10);
        decimal h_amount = _rand.RandDecimal(1.00m, 5000.00m, 2);

        int c_w_id, c_d_id;
        // TPC-C 2.5.1.2: 85% of payments are for the home warehouse
        if (_numWarehouses > 1 && _rand.RandInt(1, 100) <= 15)
        {
            do
            {
                c_w_id = _rand.RandInt(1, _numWarehouses);
            } while (c_w_id == w_id);
            c_d_id = _rand.RandInt(1, 10);
        }
        else
        {
            c_w_id = w_id;
            c_d_id = d_id;
        }

        var payment = new TPCCP()
        {
            WID = c_w_id,
            DID = c_d_id,
            CID = -1, // -1 indicates last name search
            CLAST = string.Empty, // Last name will be set later
            AMOUNT = (long)h_amount
        };

        // TPC-C 2.5.1.3: 60% of payments are by last name
        if (_rand.RandInt(1, 100) <= 60)
        {
            payment.CLAST = _rand.GetNonUniformCustomerLastNameRun();
        }
        else
        {
            payment.CID = _rand.GetCustomerId();
        }
            
        var query = new ClientRequest()
        {
            Type = QueryType.Tpccp,
            Tpccp = payment
        };
        
        return new TxInfo
        {
            Query = query,
            Type = TxType.TPCC_PA
        };
    }

}