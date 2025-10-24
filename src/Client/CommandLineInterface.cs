using System;
using System.Threading.Tasks;
using Minerva.DB_Server.Network;


namespace Minerva.Grpc_Client;



public class CommandLineInterface : IDisposable
{
    private readonly Client _client;

    public CommandLineInterface(Client client)
    {
        _client = client;
    }


    public void Start()
    {
        _client.ConnectAsync().Wait();

        Console.WriteLine("Command Format: [type]:[key],[value(if it is a set command)]");

        bool running = true;
        while (running)
        {
            string input = Console.ReadLine();
            
            if (input == "stop")
            {
                running = false;
            }
            
            
            if (!CheckQueryFormat(input))
            {
                Console.WriteLine("Invalid command format; Command Format: [type]:[key],[value(if it is a set command)]");
                continue;
            }

            // var tx = new TxInfo()
            // {
            //     Query = input
            // };

            // var res = _client.SendTransactionSync(tx).Result;


            //Console.WriteLine("Transaction complete. Status: {0} Response: {1}", res.Result.Status, res.Result.Result);
            
        }
    }


    private static bool CheckQueryFormat(string query)
    {
        if (query == "stop")
        {
            return true;
        }

        string[] parts;
        try
        {
            parts = query.Split(":");
        }
        catch (Exception)
        {
            return false;
        }

        if (parts.Length != 2)
        {
            return false;
        }

        if (parts[0] != "0" && parts[0] != "1")
        {
            return false;
        }

        return true;
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}


