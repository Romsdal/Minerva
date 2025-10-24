using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.ConflictResolver;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server;

namespace ConflictGraphBenchmark;

/// <summary>
/// Captures and parses ConflictGraphSolver timing logs
/// </summary>
public class ConflictGraphTimings
{
    public double ConstructChainsMs { get; set; }
    public double CheckStaleMs { get; set; }
    public double TrackRWSetMs { get; set; }
    public double ConstructGraphMs { get; set; }
    public double SolveMWISMs { get; set; }
    public double CompileResultsMs { get; set; }
    
    public static ConflictGraphTimings ParseFromLogs(string logOutput)
    {
        var timings = new ConflictGraphTimings();
        
        // Regex patterns to match the log entries
        var patterns = new Dictionary<string, string>
        {
            { nameof(ConstructChainsMs), @"Constructed \d+ transaction chains in (\d+) ms" },
            { nameof(CheckStaleMs), @"Check Stale in (\d+) ms" },
            { nameof(TrackRWSetMs), @"Read/Write sets tracked in (\d+) ms" },
            { nameof(ConstructGraphMs), @"Conflict graph constructed in (\d+) ms" },
            { nameof(SolveMWISMs), @"MWIS solved in (\d+) ms" },
            { nameof(CompileResultsMs), @"Results compiled in (\d+) ms" }
        };
        
        foreach (var (property, pattern) in patterns)
        {
            var match = Regex.Match(logOutput, pattern);
            if (match.Success && double.TryParse(match.Groups[1].Value, out double value))
            {
                typeof(ConflictGraphTimings).GetProperty(property)?.SetValue(timings, value);
            }
        }
        
        return timings;
    }
    
    public double TotalMs => ConstructChainsMs + CheckStaleMs + TrackRWSetMs + ConstructGraphMs + SolveMWISMs + CompileResultsMs;
}

/// <summary>
/// Custom logger that captures log output to a string
/// </summary>
public class StringCaptureLogger : ILogger
{
    private readonly StringWriter _writer = new();
    
    public string GetLogs() => _writer.ToString();
    public void ClearLogs() => _writer.GetStringBuilder().Clear();
    
    public IDisposable BeginScope<TState>(TState state) => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
    {
        var message = formatter(state, exception);
        if (message.Contains("ConflictGraphSolver"))
        {
            _writer.WriteLine(message);
        }
    }
    
    public void Dispose() => _writer.Dispose();
}

/// <summary>
/// Simplified benchmark that runs quickly and provides focused performance analysis
/// </summary>
public static class SimpleBenchmark
{
    private static readonly int[] TransactionCounts = { 10, 50, 100, 250, 500 };
    private static readonly int[] ConflictStalePercentages = { 0, 10, 20, 30 };
    private const int NumReplicas = 3;
    private const int RunsPerScenario = 5;

    public static void RunBenchmark()
    {
        Console.WriteLine("=== ConflictGraphSolver Comprehensive Performance Benchmark ===\n");
        Console.WriteLine("Testing with transaction counts: " + string.Join(", ", TransactionCounts));
        Console.WriteLine("Testing with conflict/stale percentages: " + string.Join("%, ", ConflictStalePercentages) + "%");
        Console.WriteLine("Number of replicas: " + NumReplicas);
        Console.WriteLine();

        var results = new List<BenchmarkResult>();

        int totalScenarios = TransactionCounts.Length * ConflictStalePercentages.Length * ConflictStalePercentages.Length;
        int currentScenario = 0;

        foreach (var txCount in TransactionCounts)
        {
            foreach (var conflictPercent in ConflictStalePercentages)
            {
                foreach (var stalePercent in ConflictStalePercentages)
                {
                    currentScenario++;
                    Console.Write($"[{currentScenario}/{totalScenarios}] {txCount}tx {conflictPercent}%conf {stalePercent}%stale: ");
                    
                    var result = RunMultipleBenchmarks(txCount, conflictPercent, stalePercent);
                    results.Add(result);
                    
                    Console.WriteLine($"✓");
                }
            }
        }

        Console.WriteLine("=== Performance Analysis Summary ===");
        AnalyzeResults(results);
    }

    private static BenchmarkResult RunMultipleBenchmarks(int transactionCount, int conflictPercentage, int stalePercentage)
    {
        var results = new List<BenchmarkResult>();
        
        // Run the benchmark multiple times with different random seeds for better statistical reliability
        for (int run = 0; run < RunsPerScenario; run++)
        {
            Console.Write($".");
            var result = RunSingleBenchmark(transactionCount, conflictPercentage, stalePercentage, run + 42);
            results.Add(result);
        }
        Console.Write(" "); // Add space after dots
        
        // Calculate averages
        return new BenchmarkResult
        {
            TransactionCount = transactionCount,
            ConflictPercentage = conflictPercentage,
            StalePercentage = stalePercentage,
            ExactTimeMs = results.Average(r => r.ExactTimeMs),
            GreedyTimeMs = results.Average(r => r.GreedyTimeMs),
            ExactSelected = (int)Math.Round(results.Average(r => r.ExactSelected)),
            ExactStale = (int)Math.Round(results.Average(r => r.ExactStale)),
            GreedySelected = (int)Math.Round(results.Average(r => r.GreedySelected)),
            GreedyStale = (int)Math.Round(results.Average(r => r.GreedyStale)),
            SpeedupRatio = results.Average(r => r.SpeedupRatio),
            ExactTimings = new ConflictGraphTimings
            {
                ConstructChainsMs = results.Average(r => r.ExactTimings?.ConstructChainsMs ?? 0),
                CheckStaleMs = results.Average(r => r.ExactTimings?.CheckStaleMs ?? 0),
                TrackRWSetMs = results.Average(r => r.ExactTimings?.TrackRWSetMs ?? 0),
                ConstructGraphMs = results.Average(r => r.ExactTimings?.ConstructGraphMs ?? 0),
                SolveMWISMs = results.Average(r => r.ExactTimings?.SolveMWISMs ?? 0),
                CompileResultsMs = results.Average(r => r.ExactTimings?.CompileResultsMs ?? 0)
            },
            GreedyTimings = new ConflictGraphTimings
            {
                ConstructChainsMs = results.Average(r => r.GreedyTimings?.ConstructChainsMs ?? 0),
                CheckStaleMs = results.Average(r => r.GreedyTimings?.CheckStaleMs ?? 0),
                TrackRWSetMs = results.Average(r => r.GreedyTimings?.TrackRWSetMs ?? 0),
                ConstructGraphMs = results.Average(r => r.GreedyTimings?.ConstructGraphMs ?? 0),
                SolveMWISMs = results.Average(r => r.GreedyTimings?.SolveMWISMs ?? 0),
                CompileResultsMs = results.Average(r => r.GreedyTimings?.CompileResultsMs ?? 0)
            }
        };
    }

    private static BenchmarkResult RunSingleBenchmark(int transactionCount, int conflictPercentage, int stalePercentage, int seed = 42)
    {
        var persistentStorage = new PersistentStorage();
        var solver = new ConflictGraphSolver(persistentStorage);
        var random = new Random(seed);

        // Generate test data
        var globalEpochBatches = GenerateTestBatches(transactionCount, conflictPercentage, stalePercentage, 
                                                   persistentStorage, random);

        // Benchmark exact solver with custom logger
        var exactLogger = new StringCaptureLogger();
        solver._logger = exactLogger;
        
        var sw = Stopwatch.StartNew();
        var exactResult = solver.FindConflicts(globalEpochBatches, NumReplicas, exact: true);
        sw.Stop();
        var exactTime = sw.ElapsedMilliseconds;
        var exactLogs = exactLogger.GetLogs();
        var exactTimings = ConflictGraphTimings.ParseFromLogs(exactLogs);

        // Benchmark greedy solver with custom logger
        var greedyLogger = new StringCaptureLogger();
        solver._logger = greedyLogger;
        
        sw.Restart();
        var greedyResult = solver.FindConflicts(globalEpochBatches, NumReplicas, exact: false);
        sw.Stop();
        var greedyTime = sw.ElapsedMilliseconds;
        var greedyLogs = greedyLogger.GetLogs();
        var greedyTimings = ConflictGraphTimings.ParseFromLogs(greedyLogs);

        // Clean up loggers
        exactLogger.Dispose();
        greedyLogger.Dispose();

        return new BenchmarkResult
        {
            TransactionCount = transactionCount,
            ConflictPercentage = conflictPercentage,
            StalePercentage = stalePercentage,
            ExactTimeMs = exactTime,
            GreedyTimeMs = greedyTime,
            ExactSelected = exactResult.results.Sum(r => r.Count),
            ExactStale = exactResult.stales.Sum(r => r.Count),
            GreedySelected = greedyResult.results.Sum(r => r.Count),
            GreedyStale = greedyResult.stales.Sum(r => r.Count),
            SpeedupRatio = greedyTime > 0 ? exactTime / greedyTime : double.PositiveInfinity,
            ExactTimings = exactTimings,
            GreedyTimings = greedyTimings
        };
    }

    private static List<Batch>[] GenerateTestBatches(int transactionCount, int conflictPercentage, 
        int stalePercentage, PersistentStorage persistentStorage, Random random)
    {
        var batches = new List<Batch>[NumReplicas];
        for (int i = 0; i < NumReplicas; i++)
        {
            batches[i] = new List<Batch>();
        }

        // Calculate transaction distribution
        int conflictingTxCount = (transactionCount * conflictPercentage) / 100;
        int staleTxCount = (transactionCount * stalePercentage) / 100;
        int regularTxCount = transactionCount - conflictingTxCount - staleTxCount;

        var allTransactions = new List<TransactionRecord>();
        int tidCounter = 1;

        // Generate regular transactions
        for (int i = 0; i < regularTxCount; i++)
        {
            var tx = CreateTransaction(tidCounter++, $"regular_key_{i}", $"value_{i}");
            allTransactions.Add(tx);
        }

        // Generate conflicting transactions
        var conflictKeys = new List<string>();
        int conflictGroups = Math.Max(1, Math.Min(conflictingTxCount / 2, 10));
        for (int i = 0; i < conflictGroups; i++)
        {
            conflictKeys.Add($"conflict_key_{i}");
        }

        for (int i = 0; i < conflictingTxCount; i++)
        {
            string conflictKey = conflictKeys[i % conflictKeys.Count];
            var tx = CreateTransaction(tidCounter++, conflictKey, $"conflict_value_{i}");
            allTransactions.Add(tx);
        }

        // Generate stale transactions
        for (int i = 0; i < staleTxCount; i++)
        {
            string staleKey = $"stale_key_{i}";
            SetupStaleData(staleKey, $"old_value_{i}", 5, persistentStorage);
            
            var tx = CreateStaleTransaction(tidCounter++, staleKey, 3);
            allTransactions.Add(tx);
        }

        // Distribute transactions across replicas
        DistributeTransactionsAcrossReplicas(allTransactions, batches, random);

        return batches;
    }

    private static TransactionRecord CreateTransaction(int tid, string key, string value)
    {
        var writeSetStore = new WriteSetStore();
        var readSetStore = new ReadSetStore();
        var keyAccessedFromSnapshot = new KeyAccessedFromSnapshotStore();

        writeSetStore.YCSBWriteSet[key] = value;

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

    private static TransactionRecord CreateStaleTransaction(int tid, string key, int readFromSnapshot)
    {
        var writeSetStore = new WriteSetStore();
        var readSetStore = new ReadSetStore();
        var keyAccessedFromSnapshot = new KeyAccessedFromSnapshotStore();

        readSetStore.YCSBReadKeys.Add(key);
        keyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot[key] = readFromSnapshot;

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

    private static void SetupStaleData(string key, string value, int cid, PersistentStorage persistentStorage)
    {
        var entry = new PersistentStorage.PersistEntry<string>(value, cid);
        persistentStorage.YCSBStore[key] = entry;
    }

    private static void DistributeTransactionsAcrossReplicas(List<TransactionRecord> transactions, 
        List<Batch>[] batches, Random random)
    {
        var shuffled = transactions.OrderBy(x => random.Next()).ToList();
        var batchIdCounter = 1;

        foreach (var tx in shuffled)
        {
            int replicaId = random.Next(NumReplicas);
            var batch = new Batch(batchIdCounter++, replicaId);
            
            // Occasionally create dependencies
            if (batches[replicaId].Count > 0 && random.NextDouble() < 0.2)
            {
                var lastBatch = batches[replicaId].Last();
                if (lastBatch.Transactions.Count > 0)
                {
                    var lastTx = lastBatch.Transactions.Last();
                    tx.PrevTids.Add(lastTx.Tid);
                    
                    if (lastTx.WriteSet.YCSBWriteSet.Count > 0)
                    {
                        var writtenKey = lastTx.WriteSet.YCSBWriteSet.Keys.First();
                        tx.ReadSet.YCSBReadKeys.Add(writtenKey);
                        tx.KeyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot[writtenKey] = 0;
                    }
                }
            }

            batch.Transactions.Add(tx);
            batches[replicaId].Add(batch);
        }
    }

    private static void AnalyzeResults(List<BenchmarkResult> results)
    {
        // Group by transaction count for scalability analysis
        var byTxCount = results.GroupBy(r => r.TransactionCount).OrderBy(g => g.Key);
        
        Console.WriteLine("Scalability Analysis (Average performance by transaction count):");
        Console.WriteLine("TxCount | Exact(ms) | Greedy(ms) | Speedup | Efficiency");
        Console.WriteLine("--------|-----------|------------|---------|----------");
        
        foreach (var group in byTxCount)
        {
            var avgExact = group.Average(r => r.ExactTimeMs);
            var avgGreedy = group.Average(r => r.GreedyTimeMs);
            var avgSpeedup = group.Average(r => r.SpeedupRatio);
            var avgEfficiency = group.Average(r => (double)r.GreedySelected / r.ExactSelected);
            
            Console.WriteLine($"{group.Key,7} | {avgExact,9:F2} | {avgGreedy,10:F2} | {avgSpeedup,7:F2} | {avgEfficiency,8:F2}");
        }

        Console.WriteLine();
        
        // ConflictGraphSolver Detailed Timing Analysis
        Console.WriteLine("=== ConflictGraphSolver Detailed Timing Analysis ===");
        Console.WriteLine();
        
        Console.WriteLine("Exact Solver - Average Component Timings (ms):");
        Console.WriteLine("TxCount | Chains | Stale | RWSet | Graph | MWIS | Results | Total");
        Console.WriteLine("--------|--------|-------|-------|-------|------|---------|------");
        
        foreach (var group in byTxCount)
        {
            var avgExactTimings = new ConflictGraphTimings
            {
                ConstructChainsMs = group.Average(r => r.ExactTimings?.ConstructChainsMs ?? 0),
                CheckStaleMs = group.Average(r => r.ExactTimings?.CheckStaleMs ?? 0),
                TrackRWSetMs = group.Average(r => r.ExactTimings?.TrackRWSetMs ?? 0),
                ConstructGraphMs = group.Average(r => r.ExactTimings?.ConstructGraphMs ?? 0),
                SolveMWISMs = group.Average(r => r.ExactTimings?.SolveMWISMs ?? 0),
                CompileResultsMs = group.Average(r => r.ExactTimings?.CompileResultsMs ?? 0)
            };
            
            Console.WriteLine($"{group.Key,7} | {avgExactTimings.ConstructChainsMs,6:F1} | {avgExactTimings.CheckStaleMs,5:F1} | " +
                            $"{avgExactTimings.TrackRWSetMs,5:F1} | {avgExactTimings.ConstructGraphMs,5:F1} | " +
                            $"{avgExactTimings.SolveMWISMs,4:F1} | {avgExactTimings.CompileResultsMs,7:F1} | {avgExactTimings.TotalMs,5:F1}");
        }
        
        Console.WriteLine();
        Console.WriteLine("Greedy Solver - Average Component Timings (ms):");
        Console.WriteLine("TxCount | Chains | Stale | RWSet | Graph | MWIS | Results | Total");
        Console.WriteLine("--------|--------|-------|-------|-------|------|---------|------");
        
        foreach (var group in byTxCount)
        {
            var avgGreedyTimings = new ConflictGraphTimings
            {
                ConstructChainsMs = group.Average(r => r.GreedyTimings?.ConstructChainsMs ?? 0),
                CheckStaleMs = group.Average(r => r.GreedyTimings?.CheckStaleMs ?? 0),
                TrackRWSetMs = group.Average(r => r.GreedyTimings?.TrackRWSetMs ?? 0),
                ConstructGraphMs = group.Average(r => r.GreedyTimings?.ConstructGraphMs ?? 0),
                SolveMWISMs = group.Average(r => r.GreedyTimings?.SolveMWISMs ?? 0),
                CompileResultsMs = group.Average(r => r.GreedyTimings?.CompileResultsMs ?? 0)
            };
            
            Console.WriteLine($"{group.Key,7} | {avgGreedyTimings.ConstructChainsMs,6:F1} | {avgGreedyTimings.CheckStaleMs,5:F1} | " +
                            $"{avgGreedyTimings.TrackRWSetMs,5:F1} | {avgGreedyTimings.ConstructGraphMs,5:F1} | " +
                            $"{avgGreedyTimings.SolveMWISMs,4:F1} | {avgGreedyTimings.CompileResultsMs,7:F1} | {avgGreedyTimings.TotalMs,5:F1}");
        }

        Console.WriteLine();
        
        // Conflict/Stale impact analysis
        Console.WriteLine("Conflict/Stale Impact Analysis (500 transactions):");
        var largeScale = results.Where(r => r.TransactionCount == 500).ToList();
        if (largeScale.Any())
        {
            Console.WriteLine("Conflict% | Stale% | Exact(ms) | Greedy(ms) | Selected | Stale");
            Console.WriteLine("----------|--------|-----------|------------|----------|------");
            
            foreach (var result in largeScale.OrderBy(r => r.ConflictPercentage).ThenBy(r => r.StalePercentage))
            {
                Console.WriteLine($"{result.ConflictPercentage,9} | {result.StalePercentage,6} | {result.ExactTimeMs,9:F2} | " +
                                $"{result.GreedyTimeMs,10:F2} | {result.ExactSelected,8} | {result.ExactStale,5}");
            }
        }
    }
}

public class BenchmarkResult
{
    public int TransactionCount { get; set; }
    public int ConflictPercentage { get; set; }
    public int StalePercentage { get; set; }
    public double ExactTimeMs { get; set; }
    public double GreedyTimeMs { get; set; }
    public double SpeedupRatio { get; set; }
    public int ExactSelected { get; set; }
    public int ExactStale { get; set; }
    public int GreedySelected { get; set; }
    public int GreedyStale { get; set; }
    public ConflictGraphTimings? ExactTimings { get; set; }
    public ConflictGraphTimings? GreedyTimings { get; set; }
}