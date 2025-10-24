// See https://aka.ms/new-console-template for more information
using System;
using System.Diagnostics;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;
using ProtoBuf;


namespace Minerva.DB_Server;

class Program
{
    static void Main(string[] args)
    {

        // Process proc = Process.GetCurrentProcess();
        // long affinityMask = 0x00F; // Use CPU cores 0, 1, 2, and 3
        // proc.ProcessorAffinity = (IntPtr)affinityMask;
        ThreadPool.SetMaxThreads(1000, 100);




        string MinervaConfigPath = args[0];
        string NodeConfigPath = args[1];
        string LoggerConfig = args[2];

        LoggerManager.ConfigureLogger(LoggerConfig);
        MinervaConfig config = MinervaConfig.ParseConfigJson(MinervaConfigPath);
        NodeInfo[] nodes = NodeInfo.ParseConfigJson(NodeConfigPath);

        MinervaService service = new(config, nodes);
        service.Init();


        Console.CancelKeyPress += delegate(object sender, ConsoleCancelEventArgs e)
        {
            service.StopInterface();
        };


        service.StartInterface(InterfaceType.GRPC);
        // whenever the interface stops
        service.Dispose();
    }
}