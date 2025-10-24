using System;
using Minerva.DB_Server.Benchmarks;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.QueryExecutor;



/// <summary>
/// I am not actually gonna write a SQL parser...
/// </summary>
public class BenchmarkQueryParser : IQueryParser
{

    public IExecutionPlan ParseSQL(ClientRequest query, ITransactionManager transactionManager)
    {

        switch (query.Type)
        {
            case QueryType.Ycsb:
                return new BasicTxs(query, transactionManager);
            case QueryType.Tpccno:
                return new TxnNewOrder(query, transactionManager);
            case QueryType.Tpccp:
                return new TxnPayment(query, transactionManager);
            // case QueryType.Tpccos:
            //     // Handle TPPCPP query
            //     break;
            // case QueryType.Tpccsl:
            //     // Handle TPCCP query
            //     break;
            // case QueryType.Tpccd:
            //     // Handle TPCCSL query
            //     break;
            case QueryType.Tpccli:
                return new LoadItems(query, transactionManager);
            case QueryType.Tpcclw:
                return new LoadWarehouses(query, transactionManager);
            case QueryType.Tpccld:
                return new LoadDistricts(query, transactionManager);
            case QueryType.Tpcclc:
                return new LoadCustomers(query, transactionManager);
            case QueryType.Tpccls:
                return new LoadStock(query, transactionManager);
            case QueryType.Tpcclio:
                return new LoadInitialOrders(query, transactionManager);
            case QueryType.Tpcclh:
                return new LoadInitialHistory(query, transactionManager);
            default:
                throw new ArgumentException("Unknown query type.");
        }

    }


}