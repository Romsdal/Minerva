using System;
using System.Threading.Tasks;
using Minerva.DB_Server;
using Minerva.DB_Server.Interface;
namespace Minerva.DB_server.Interface;

public sealed class CLIInterface : IDBInterface
{
    private static CLIInterface _instance;

    private readonly QueryHandler _queryHandler;

    private volatile bool _running = true;

    private CLIInterface(QueryHandler queryHandler)
    {
        _queryHandler = queryHandler ?? throw new ArgumentNullException(nameof(queryHandler));
    }

    public static CLIInterface GetInstance(QueryHandler queryHandler)
    {
        if (_instance == null)
        {
            if (_instance == null)
            {
                _instance = new CLIInterface(queryHandler);
            }
            
        }

        return _instance;
    }


    public void Start()
    {

        while (_running)
        {

            Console.Write("Enter query: ");
            string input = Console.ReadLine();

            if (input == "exit")
            {
                Console.WriteLine("Exiting...");
                break;
            }
            else
            {
                //var tx = await _queryHandler.ReceivedQuery(input);
                //Console.WriteLine($"Transaction Status: {tx.Status}, Transaction ID: {tx.Tid}, Result: {tx.Result}");
            }
        }

    }

    public void Stop()
    {
        _running = false;
    }
}