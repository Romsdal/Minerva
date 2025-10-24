using System;
using System.Threading;
using System.Threading.Tasks;
using Minerva.DB_Server.Interface;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_server.Interface;

public sealed class TestInterface : IDBInterface
{
    private readonly QueryHandler _queryHandler;

    private TestInterface(QueryHandler queryHandler)
    {
        _queryHandler = queryHandler ?? throw new ArgumentNullException(nameof(queryHandler));
    }

    public static TestInterface GetInstance(QueryHandler queryHandler)
    {
        return new TestInterface(queryHandler);
    }

    volatile bool _running = true;
    public void Start()
    {
        while (_running)
        {
            Thread.Sleep(500);
        }
    }


    // public async Task<(bool, string)> NewQuery(ClientRequest query)
    // {
    //     return await _queryHandler.ReceivedQuery(query);
    // }

    public void Stop()
    {
        _running = false;
    }

}