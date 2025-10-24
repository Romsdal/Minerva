using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace OCCBenchmark;

public class Program
{
    public static async Task Main(string[] args)
    {
        Process proc = Process.GetCurrentProcess();
        long affinityMask = 0xF00; // Use CPU cores 8, 9, 10, and 11
        proc.ProcessorAffinity = (IntPtr)affinityMask;


        var benchmark = new OCCThroughputBenchmark();
        await benchmark.RunThroughputBenchmark();
    }
}

public class OCCThroughputBenchmark
{

    private MinervaTxOCCExecutor _occExecutor = null!;
    private PersistentStorage _storage = null!;
    private BenchmarkQueryParser _queryParser = null!;
    private readonly Random _random = new(42);

    // Metrics tracking
    private long _totalTransactions = 0;
    private long _successfulTransactions = 0;
    private long _failedTransactions = 0;
    private readonly ConcurrentQueue<double> _latencies = new();

    // Control flags
    private volatile bool _shouldStop = false;
    private readonly object _metricsLock = new object();

    public async Task RunThroughputBenchmark()
    {
        Setup();

        Console.WriteLine("OCC Throughput Benchmark");
        Console.WriteLine("========================");
        Console.WriteLine("Running for 30 seconds with maximum throughput...");
        Console.WriteLine("Database: 10,000 preloaded keys with 50% contention ratio");
        Console.WriteLine("Reporting throughput every second\n");

        // Start the benchmark timer
        var stopwatch = Stopwatch.StartNew();

        // Start metrics reporting task
        var metricsTask = ReportMetricsEverySecond();

        // Start multiple worker tasks for maximum throughput
        var workerTasks = new List<Task>();
        int workerCount = Environment.ProcessorCount * 2; // 2x CPU cores for optimal throughput

        Console.WriteLine($"Starting {workerCount} worker threads for maximum throughput...\n");

        for (int i = 0; i < workerCount; i++)
        {
            int workerId = i;
            workerTasks.Add(Task.Run(() => WorkerThread(workerId)));
        }

        // Run for 30 seconds
        await Task.Delay(30000);

        // Signal workers to stop
        _shouldStop = true;

        // Wait for all workers to complete
        await Task.WhenAll(workerTasks);

        stopwatch.Stop();

        // Wait for final metrics report
        await Task.Delay(1000);

        // Print final summary
        PrintFinalSummary(stopwatch.Elapsed);
    }

    private void Setup()
    {
        _storage = new PersistentStorage();
        _queryParser = new BenchmarkQueryParser();
        _occExecutor = new MinervaTxOCCExecutor(_queryParser, _storage);
        _occExecutor.SetNewTempStates(new List<string> { "benchmark" });
        
        // Preload database with 10k keys to ensure realistic read scenarios
        Console.WriteLine("Preloading database with 10,000 keys...");
        PreloadDatabase();
        Console.WriteLine("Database preload completed.\n");
    }

    private void PreloadDatabase()
    {
        const int keyCount = 10000;
        
        // Create preload transactions in batches to avoid memory issues
        const int batchSize = 1000;
        
        for (int batch = 0; batch < keyCount / batchSize; batch++)
        {
            var preloadQuery = new ClientRequest
            {
                Type = QueryType.Ycsb,
                SeqId = (uint)(batch + 1000000), // Use high SeqId to avoid conflicts
                KVCmds = new List<KV>()
            };
            
            // Add keys for this batch
            for (int i = 0; i < batchSize; i++)
            {
                int keyId = batch * batchSize + i;
                var key = $"key_{keyId:D6}";
                var value = $"preload_value_{keyId}_" + new string('x', 80); // ~100 char values
                
                preloadQuery.KVCmds.Add(new KV
                {
                    Type = OpType.Set,
                    Key = key,
                    Value = value
                });
            }
            
            // Execute preload transaction
            try
            {
                _occExecutor.OCCExecuteSingle(preloadQuery);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Failed to preload batch {batch}: {ex.Message}");
            }
        }
    }

    private void WorkerThread(int workerId)
    {
        while (!_shouldStop)
        {
            try
            {
                // Create a random transaction
                var query = CreateRandomYCSBQuery();

                // Measure latency
                var sw = Stopwatch.StartNew();

                var tx = _occExecutor.OCCExecuteSingle(query);

                sw.Stop();

                // Record metrics
                Interlocked.Increment(ref _totalTransactions);

                if (tx?.Status == TransactionStatus.LocalCompleted)
                {
                    Interlocked.Increment(ref _successfulTransactions);
                    _latencies.Enqueue(sw.Elapsed.TotalMicroseconds);
                }
                else
                {
                    Interlocked.Increment(ref _failedTransactions);
                }

                // Optional: Small delay to prevent overwhelming the system
                // Thread.Sleep(1); // Uncomment if needed to reduce CPU usage
            }
            catch (Exception)
            {
                Interlocked.Increment(ref _failedTransactions);
                // Don't spam console with errors in throughput test
            }
        }
    }

    private async Task ReportMetricsEverySecond()
    {
        long lastTotalTransactions = 0;
        long lastSuccessfulTransactions = 0;
        int secondsElapsed = 0;

        while (!_shouldStop)
        {
            await Task.Delay(1000);
            secondsElapsed++;

            lock (_metricsLock)
            {
                long currentTotal = _totalTransactions;
                long currentSuccessful = _successfulTransactions;
                long currentFailed = _failedTransactions;

                long tpsTotal = currentTotal - lastTotalTransactions;
                long tpsSuccessful = currentSuccessful - lastSuccessfulTransactions;

                // Calculate average latency for this second
                var latencySum = 0.0;
                var latencyCount = 0;
                while (_latencies.TryDequeue(out var latency))
                {
                    latencySum += latency;
                    latencyCount++;
                }

                var avgLatency = latencyCount > 0 ? latencySum / latencyCount : 0.0;
                var successRate = currentTotal > 0 ? (double)currentSuccessful / currentTotal * 100 : 0;

                Console.WriteLine($"[{secondsElapsed:D2}s] " +
                    $"TPS: {tpsTotal,5} | " +
                    $"Success TPS: {tpsSuccessful,5} | " +
                    $"Total: {currentTotal,7} | " +
                    $"Success Rate: {successRate,5:F1}% | " +
                    $"Avg Latency: {avgLatency,6:F0}μs");

                lastTotalTransactions = currentTotal;
                lastSuccessfulTransactions = currentSuccessful;
            }
        }
    }

    static readonly string val = new string('x', 1000);
    private ClientRequest CreateRandomYCSBQuery()
    {
        var query = new ClientRequest
        {
            Type = QueryType.Ycsb,
            SeqId = (uint)_random.Next(1, 1000000),
            KVCmds = new List<KV>()
        };

        // Use 10k key space but with 50% contention pattern
        const int totalKeySpace = 10000;
        const int contentionKeySpace = totalKeySpace / 100; // 1000 keys for 10% contention
        
        var operationCount = _random.Next(5, 15); // 5-15 operations per transaction

        for (int i = 0; i < operationCount; i++)
        {
            // 50% of operations use contentious keys (0-4999), 50% use non-contentious keys (5000-9999)
            int keyId;
            if (_random.NextDouble() < 0.5)
            {
                // Contentious keys - first half of key space (high contention)
                keyId = _random.Next(0, contentionKeySpace);
            }
            else
            {
                // Non-contentious keys - second half of key space (low contention)
                keyId = _random.Next(contentionKeySpace, totalKeySpace);
            }
            
            var key = $"key_{keyId:D6}";

            if (_random.NextDouble() < 0.5) // 50% reads, 50% writes
            {
                query.KVCmds.Add(new KV
                {
                    Type = OpType.Get,
                    Key = key,
                    Value = ""
                });
            }
            else
            {
                query.KVCmds.Add(new KV
                {
                    Type = OpType.Set,
                    Key = key,
                    Value = val
                });
            }
        }

        return query;
    }



    private void PrintFinalSummary(TimeSpan elapsed)
    {
        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("FINAL SUMMARY");
        Console.WriteLine(new string('=', 60));

        var totalSeconds = elapsed.TotalSeconds;
        var avgThroughput = _totalTransactions / totalSeconds;
        var avgSuccessfulThroughput = _successfulTransactions / totalSeconds;
        var overallSuccessRate = _totalTransactions > 0 ? (double)_successfulTransactions / _totalTransactions * 100 : 0;

        Console.WriteLine($"Total Runtime:           {elapsed.TotalSeconds:F1} seconds");
        Console.WriteLine($"Total Transactions:      {_totalTransactions:N0}");
        Console.WriteLine($"Successful Transactions: {_successfulTransactions:N0}");
        Console.WriteLine($"Failed Transactions:     {_failedTransactions:N0}");
        Console.WriteLine($"Overall Success Rate:    {overallSuccessRate:F1}%");
        Console.WriteLine($"Average Total TPS:       {avgThroughput:F1}");
        Console.WriteLine($"Average Success TPS:     {avgSuccessfulThroughput:F1}");
        Console.WriteLine($"Peak Theoretical TPS:    {_totalTransactions} (if all in 1 second)");
    }
}
