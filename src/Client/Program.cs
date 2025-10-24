using System;
using System.Diagnostics;
using Minerva.DB_Server.Network.Protos;
using ProtoBuf;

namespace Minerva.Grpc_Client;

public class Program
{
    public static void Main(string[] args)
    {

        // Process proc = Process.GetCurrentProcess();
        // long affinityMask = 0xF00; // Use CPU cores 8, 9, 10, and 11
        // proc.ProcessorAffinity = (IntPtr)affinityMask;


        Serializer.PrepareSerializer<ClientRequest>();
        Serializer.PrepareSerializer<TxResult>();


        if (args.Length != 1)
        {
            Console.WriteLine("Usage: GrpcClient <benchmarkConfigFilePath> or [\"i\" for interactive mode]");
            return;
        }

        if (args[0] == "i")
        {
            Console.WriteLine("Please input server address:");
            string serverAddress = Console.ReadLine();
            Console.WriteLine("Please input server port:");
            string portInput = Console.ReadLine();

            var client = new Client(serverAddress, int.Parse(portInput));
            using var cli = new CommandLineInterface(client);
            cli.Start();
        }
        else
        {
            string configFilePath = args[0];
            BenchmarkConfig config = BenchmarkConfig.ParseConfigFromJson(configFilePath);
            BenchmarkInterface benchmarkInterface = new(config);
            benchmarkInterface.StartBenchmark();
        }
    }
}