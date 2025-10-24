using System;
using System.Collections.Generic;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.QueryExecutor;

public interface IExecutionPlan
{
    public Transaction Execute();
}


public abstract class ExecutionPlan(ClientRequest query, ITransactionManager transactionManager) : IExecutionPlan
{
    protected ClientRequest _query = query;
    protected ITransactionManager _transactionManager = transactionManager;

    public abstract Transaction Execute();

}