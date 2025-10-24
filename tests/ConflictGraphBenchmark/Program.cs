using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using Minerva.DB_Server.ConflictResolver;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.MinervaLog;

namespace ConflictGraphBenchmark;

[Config(typeof(BenchmarkConfig))]
public class ConflictGraphSolverBenchmark
{
    private ConflictGraphSolver _solver = null!;
    private PersistentStorage _persistentStorage = null!;
    private Random _random = null!;

    // Benchmark parameters
    [Params(10, 50, 100, 250, 500, 1000)]
    public int TransactionCount { get; set; }

    [Params(0, 10, 20, 30)]
    public int ConflictPercentage { get; set; }

    [Params(0, 10, 20, 30)]
    public int StalePercentage { get; set; }

    private List<Batch>[] _globalEpochBatches = null!;
    private const int NumReplicas = 3;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _persistentStorage = new PersistentStorage();
        _solver = new ConflictGraphSolver(_persistentStorage);
        _random = new Random(42); // Fixed seed for reproducible results
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Generate test data for each iteration
        _globalEpochBatches = GenerateTestBatches(TransactionCount, ConflictPercentage, StalePercentage);
    }

    [Benchmark]
    public (List<TransactionRecord>[], List<TransactionRecord>[]) BenchmarkExactSolver()
    {
        return _solver.FindConflicts(_globalEpochBatches, NumReplicas, exact: true);
    }

    [Benchmark]
    public (List<TransactionRecord>[], List<TransactionRecord>[]) BenchmarkGreedySolver()
    {
        return _solver.FindConflicts(_globalEpochBatches, NumReplicas, exact: false);
    }

    /// <summary>
    /// Generates test batches with specified conflict and stale percentages
    /// </summary>
    private List<Batch>[] GenerateTestBatches(int transactionCount, int conflictPercentage, int stalePercentage)
    {
        var batches = new List<Batch>[NumReplicas];
        for (int i = 0; i < NumReplicas; i++)
        {
            batches[i] = new List<Batch>();
        }

        // Calculate number of conflicting and stale transactions
        int conflictingTxCount = (transactionCount * conflictPercentage) / 100;
        int staleTxCount = (transactionCount * stalePercentage) / 100;
        int regularTxCount = transactionCount - conflictingTxCount - staleTxCount;

        var allTransactions = new List<TransactionRecord>();
        int tidCounter = 1;

        // Generate regular (non-conflicting, non-stale) transactions
        for (int i = 0; i < regularTxCount; i++)
        {
            var tx = CreateTransaction(tidCounter++, $"regular_key_{i}", $"value_{i}");
            allTransactions.Add(tx);
        }

        // Generate conflicting transactions (multiple transactions accessing same keys)
        var conflictKeys = new List<string>();
        for (int i = 0; i < Math.Min(conflictingTxCount / 2, 10); i++) // Limit conflict groups
        {
            conflictKeys.Add($"conflict_key_{i}");
        }

        for (int i = 0; i < conflictingTxCount; i++)
        {
            string conflictKey = conflictKeys[i % conflictKeys.Count];
            var tx = CreateTransaction(tidCounter++, conflictKey, $"conflict_value_{i}");
            allTransactions.Add(tx);
        }

        // Generate stale transactions (transactions reading outdated data)
        for (int i = 0; i < staleTxCount; i++)
        {
            string staleKey = $"stale_key_{i}";
            // Set up persistent data that was updated more recently than the read snapshot
            SetupStaleData(staleKey, $"old_value_{i}", 5); // Data updated at commit 5
            
            var tx = CreateStaleTransaction(tidCounter++, staleKey, 3); // Read from older snapshot
            allTransactions.Add(tx);
        }

        // Distribute transactions across replicas
        DistributeTransactionsAcrossReplicas(allTransactions, batches);

        return batches;
    }

    /// <summary>
    /// Creates a regular transaction
    /// </summary>
    private TransactionRecord CreateTransaction(int tid, string key, string value, bool isRead = false)
    {
        var writeSetStore = new WriteSetStore();
        var readSetStore = new ReadSetStore();
        var keyAccessedFromSnapshot = new KeyAccessedFromSnapshotStore();

        if (isRead)
        {
            readSetStore.YCSBReadKeys.Add(key);
            keyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot[key] = 0; // Current snapshot
        }
        else
        {
            writeSetStore.YCSBWriteSet[key] = value;
        }

        return new TransactionRecord
        {
            Tid = tid,
            PrevTids = new List<int>(),
            Query = new ClientRequest { Type = QueryType.Ycsb, SeqId = (uint)tid },
            WriteSet = writeSetStore,
            ReadSet = readSetStore,
            KeyAccessedFromSnapshot = keyAccessedFromSnapshot
        };
    }

    /// <summary>
    /// Creates a stale transaction (reads outdated data)
    /// </summary>
    private TransactionRecord CreateStaleTransaction(int tid, string key, int readFromSnapshot)
    {
        var writeSetStore = new WriteSetStore();
        var readSetStore = new ReadSetStore();
        var keyAccessedFromSnapshot = new KeyAccessedFromSnapshotStore();

        readSetStore.YCSBReadKeys.Add(key);
        keyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot[key] = readFromSnapshot; // Older snapshot

        return new TransactionRecord
        {
            Tid = tid,
            PrevTids = new List<int>(),
            Query = new ClientRequest { Type = QueryType.Ycsb, SeqId = (uint)tid },
            WriteSet = writeSetStore,
            ReadSet = readSetStore,
            KeyAccessedFromSnapshot = keyAccessedFromSnapshot
        };
    }

    /// <summary>
    /// Sets up persistent data for stale transaction testing
    /// </summary>
    private void SetupStaleData(string key, string value, int cid)
    {
        var entry = new PersistentStorage.PersistEntry<string>(value, cid);
        _persistentStorage.YCSBStore[key] = entry;
    }

    /// <summary>
    /// Distributes transactions across replicas in batches
    /// </summary>
    private void DistributeTransactionsAcrossReplicas(List<TransactionRecord> transactions, List<Batch>[] batches)
    {
        // Shuffle transactions for realistic distribution
        var shuffled = transactions.OrderBy(x => _random.Next()).ToList();
        
        // Create dependency chains within replicas occasionally
        var replicaTxCounts = new int[NumReplicas];
        var batchIdCounter = 1;

        foreach (var tx in shuffled)
        {
            int replicaId = _random.Next(NumReplicas);
            
            // Create a new batch for this replica
            var batch = new Batch(batchIdCounter++, replicaId);
            
            // Occasionally create transaction chains (dependencies)
            if (replicaTxCounts[replicaId] > 0 && _random.NextDouble() < 0.3) // 30% chance of dependency
            {
                var existingBatches = batches[replicaId];
                if (existingBatches.Count > 0)
                {
                    var lastBatch = existingBatches.Last();
                    if (lastBatch.Transactions.Count > 0)
                    {
                        var lastTx = lastBatch.Transactions.Last();
                        tx.PrevTids.Add(lastTx.Tid);
                        
                        // Add read dependency to create a chain
                        if (lastTx.WriteSet.YCSBWriteSet.Count > 0)
                        {
                            var writtenKey = lastTx.WriteSet.YCSBWriteSet.Keys.First();
                            tx.ReadSet.YCSBReadKeys.Add(writtenKey);
                            tx.KeyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot[writtenKey] = 0;
                        }
                    }
                }
            }

            batch.Transactions.Add(tx);
            batches[replicaId].Add(batch);
            replicaTxCounts[replicaId]++;
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        // ConflictGraphSolver doesn't implement IDisposable, so no cleanup needed
    }
}

/// <summary>
/// Custom benchmark configuration
/// </summary>
public class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        AddJob(Job.Default
            .WithWarmupCount(2)
            .WithIterationCount(3)
            .WithUnrollFactor(16)
            .WithInvocationCount(16));
        
        // Use default exporters that are available
        AddLogger(ConsoleLogger.Default);
    }
}

/// <summary>
/// Performance analysis helper
/// </summary>
public static class PerformanceAnalyzer
{
    public static void RunCustomAnalysis()
    {
        Console.WriteLine("=== ConflictGraphSolver Performance Analysis ===\n");
        
        var storage = new PersistentStorage();
        var solver = new ConflictGraphSolver(storage);
        var scenarios = new[]
        {
            new { TxCount = 10, Conflict = 0, Stale = 0, Name = "Small Scale - No Conflicts" },
            new { TxCount = 100, Conflict = 10, Stale = 10, Name = "Medium Scale - Low Conflicts" },
            new { TxCount = 500, Conflict = 20, Stale = 20, Name = "Large Scale - Medium Conflicts" },
            new { TxCount = 1000, Conflict = 30, Stale = 30, Name = "Very Large Scale - High Conflicts" }
        };

        foreach (var scenario in scenarios)
        {
            Console.WriteLine($"Testing: {scenario.Name}");
            Console.WriteLine($"Transactions: {scenario.TxCount}, Conflicts: {scenario.Conflict}%, Stale: {scenario.Stale}%");
            
            var benchmark = new ConflictGraphSolverBenchmark
            {
                TransactionCount = scenario.TxCount,
                ConflictPercentage = scenario.Conflict,
                StalePercentage = scenario.Stale
            };
            
            benchmark.GlobalSetup();
            benchmark.IterationSetup();
            
            // Measure exact solver
            var sw = Stopwatch.StartNew();
            var exactResult = benchmark.BenchmarkExactSolver();
            sw.Stop();
            var exactTime = sw.ElapsedMilliseconds;
            
            // Measure greedy solver
            sw.Restart();
            var greedyResult = benchmark.BenchmarkGreedySolver();
            sw.Stop();
            var greedyTime = sw.ElapsedMilliseconds;
            
            // Analyze results
            var exactSelected = exactResult.Item1.Sum(r => r.Count);
            var exactStale = exactResult.Item2.Sum(r => r.Count);
            var greedySelected = greedyResult.Item1.Sum(r => r.Count);
            var greedyStale = greedyResult.Item2.Sum(r => r.Count);
            
            Console.WriteLine($"  Exact Solver:  {exactTime}ms, Selected: {exactSelected}, Stale: {exactStale}");
            Console.WriteLine($"  Greedy Solver: {greedyTime}ms, Selected: {greedySelected}, Stale: {greedyStale}");
            Console.WriteLine($"  Speedup: {(double)exactTime / greedyTime:F2}x");
            Console.WriteLine();
            
            // No cleanup needed as ConflictGraphSolver doesn't implement IDisposable
        }
    }
}

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("ConflictGraphSolver Benchmark Suite");
        Console.WriteLine("===================================\n");
        
        if (args.Length > 0 && args[0] == "quick")
        {
            Console.WriteLine("Running quick performance analysis...\n");
            PerformanceAnalyzer.RunCustomAnalysis();
        }
        else if (args.Length > 0 && args[0] == "full")
        {
            Console.WriteLine("Running full BenchmarkDotNet suite...");
            Console.WriteLine("This will take several minutes to complete.\n");
            
            var summary = BenchmarkRunner.Run<ConflictGraphSolverBenchmark>();
            
            Console.WriteLine("\n=== Benchmark Summary ===");
            Console.WriteLine($"Total benchmarks run: {summary.Reports.Length}");
            Console.WriteLine($"Successful runs: {summary.Reports.Count(r => r.Success)}");
            Console.WriteLine($"Failed runs: {summary.Reports.Count(r => !r.Success)}");
        }
        else
        {
            Console.WriteLine("Running comprehensive simplified benchmark...");
            Console.WriteLine("This will test all scenarios efficiently.\n");
            
            SimpleBenchmark.RunBenchmark();
        }
        
        Console.WriteLine("\nBenchmark completed. Check the output above for detailed results.");
    }
}
