using System;
using System.Text;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.Benchmarks;

public class BasicTxs(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{
    public override Transaction Execute()
    {
        var tx = _transactionManager.CreateTransaction();

    

        StringBuilder results = new();
        tx.Begin();

        foreach (var arg in _query.KVCmds)
        {
            if (arg.Type == OpType.Get) // it is get
            {
                var shard = arg.Shard;
                var key = arg.Key;

                if (tx.KeyGet(shard, key, out var value))
                {
                    //results.Append(value); 
                    results.Append("OK;");
                }
                else
                {
                    Console.WriteLine($"Key {key} not found in shard {shard}");
                    results.Append("DNF;");
                }
            }
            else // it is set
            {
                var shard = arg.Shard;
                var key = arg.Key;
                var value = arg.Value;
                tx.KeySet(shard, key, value);
                results.Append("OK;");
            }

        }

        tx.Complete();
        tx.Result = results.ToString();

        return tx;
    }
}