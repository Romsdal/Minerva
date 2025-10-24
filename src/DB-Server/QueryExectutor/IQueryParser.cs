
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.QueryExecutor;

/// <summary>
/// Query Parser interface convert SQL into a series of operations.
/// </summary>
public interface IQueryParser
{
    IExecutionPlan ParseSQL(ClientRequest query, ITransactionManager transactionManager);
}