using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.Interface;

public class ProtoClientInterface : IDBInterface
{
    private readonly QueryHandler _queryHandler;
    private readonly ILogger _logger = LoggerManager.GetLogger();
    private readonly ManualResetEventSlim _stopEvent = new(false);

    private ProtoClientInterface(QueryHandler queryHandler)
    {
        _queryHandler = queryHandler ?? throw new ArgumentNullException(nameof(queryHandler));
    }

    public static ProtoClientInterface GetInstance(QueryHandler queryHandler)
    {
        return new ProtoClientInterface(queryHandler);
    }

    public void Start()
    {
        _stopEvent.Wait();
    }

    public void Stop()
    {
        if (!_stopEvent.IsSet)
        {
            _stopEvent.Set();
            _logger.LogWarning("Interface Stopped.");
        }
    }

    public async Task<TxResult> NewQuery(ClientRequest query)
    {
        if (query.Type == QueryType.Stop)
        {
            Stop();
            return new TxResult { SeqId = query.SeqId, Executed = true, TxResultStr = "Server Stopped" };
        }
        else if (query.Type == QueryType.Stats)
        {
            return new TxResult { SeqId = query.SeqId, Executed = true, TxResultStr = Stats.GetStats() };
        }
        else if (query.Type == QueryType.SaveState)
        {
            try
            {

                _queryHandler.SaveStorageToDisk();
                return new TxResult { SeqId = query.SeqId, Executed = true, TxResultStr = "Storage saved to disk" };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving storage to disk");
                return new TxResult { SeqId = query.SeqId, Executed = false, TxResultStr = $"Error saving storage to disk: {ex.Message}" };
            }
        }
        else
        {
            var (executed, result) = await _queryHandler.ReceivedQuery(query);

            return new TxResult { SeqId = query.SeqId, Executed = executed, TxResultStr = result };
        }
    }

}


