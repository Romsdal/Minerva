using System;
using System.Threading;
using System.Threading.Tasks;

namespace Minerva.Grpc_Client;

public class BenchmarkInterface
{
    public BenchmarkConfig Config { get; init; }
    public BenchmarkRunner runner { get; init; }

    public BenchmarkInterface(BenchmarkConfig config)
    {
        Config = config;

        IBenchmarkWorkload workload = null;

        if (Config.Type == BenchmarkType.YCSB)
        {
            workload = new YCSB(config.YCSBConfig, config.PreLoadDB);

        }
        else if (Config.Type == BenchmarkType.TPCC)
        {
            workload = new TPCC(config.TPCCConfig);
        }
        else
        {
            throw new NotSupportedException($"Benchmark type '{Config.Type}' is not supported.");
        }
        
        runner = new BenchmarkRunner(config, config.Servers, workload);

    }

    public void StartBenchmark()
    {

        Console.WriteLine("Starting benchmark: {0}", Config.BenchmarkName);
        
        if (!Config.PreLoadDB)
        {
            runner.LoadDatabase();
            Thread.Sleep(3000); // Wait for the database to load
        }

        

        //runner.SaveDatabase().Wait();

       BenchmarkResult result = runner.RunBenchmark();

       result.ParseAndPrintResults();
    }
}