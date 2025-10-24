using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace DeterExecTest;

public class OCCBenchmark
{
    private MinervaTxOCCExecutor _occExecutor = null!;
    private PersistentStorage _storage = null!;
    private BenchmarkQueryParser _queryParser = null!;
    private List<ClientRequest> _queries = null!;
    private int _completedTransactions;

    public async Task RunBenchmark()
    {
        Setup();
        
        Console.WriteLine("\nMinervaTx OCC Executor Performance Benchmark");
        Console.WriteLine("===========================================");
        Console.WriteLine("Testing sequential vs parallel OCC execution\n");
        
        var transactionCounts = new int[] { 10, 50, 100, 200, 400, 800 };
        
        foreach (var txCount in transactionCounts)
        {
            await BenchmarkTransactionCount(txCount);
        }
        
        Cleanup();
    }

    private async Task BenchmarkTransactionCount(int transactionCount)
    {
        Console.WriteLine($"Transaction Count: {transactionCount}");
        Console.WriteLine("---------------------------");
        
        var sequentialTimes = new List<long>();
        var parallelTimes = new List<long>();
        
        // Run multiple iterations for more accurate results (reduced for higher tx counts)
        var iterations = transactionCount > 100 ? 2 : 3;
        for (int iteration = 0; iteration < iterations; iteration++)
        {
            Console.WriteLine($"  Iteration {iteration + 1}/{iterations}...");
            
            try
            {
                // Sequential OCC execution
                CreateBenchmarkQueries(transactionCount);
                _occExecutor.SetNewTempStates(); // Reset temp state
                var sw = Stopwatch.StartNew();
                await ExecuteSequentialOCC();
                sw.Stop();
                sequentialTimes.Add(sw.ElapsedMilliseconds);
                Console.WriteLine($"    Sequential OCC: {sw.ElapsedMilliseconds}ms");
                
                // Parallel OCC execution with timeout
                CreateBenchmarkQueries(transactionCount);
                _occExecutor.SetNewTempStates(); // Reset temp state
                sw.Restart();
                
                var parallelTask = ExecuteParallelOCC();
                var timeoutMs = Math.Max(30000, transactionCount * 200); // Dynamic timeout
                var timeoutTask = Task.Delay(timeoutMs);
                
                var completedTask = await Task.WhenAny(parallelTask, timeoutTask);
                
                if (completedTask == timeoutTask)
                {
                    Console.WriteLine($"    Parallel OCC: TIMEOUT (>{timeoutMs}ms) - likely deadlock or too slow");
                    parallelTimes.Add(timeoutMs); // Use timeout value as penalty
                }
                else
                {
                    sw.Stop();
                    parallelTimes.Add(sw.ElapsedMilliseconds);
                    Console.WriteLine($"    Parallel OCC: {sw.ElapsedMilliseconds}ms");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"    Error in iteration {iteration + 1}: {ex.Message}");
                break;
            }
        }
        
        if (sequentialTimes.Count > 0 && parallelTimes.Count > 0)
        {
            var avgSequential = sequentialTimes.Sum() / sequentialTimes.Count;
            var avgParallel = parallelTimes.Sum() / parallelTimes.Count;
            
            Console.WriteLine($"  Sequential OCC Average: {avgSequential}ms");
            Console.WriteLine($"  Parallel OCC Average:   {avgParallel}ms");
            
            if (avgSequential < avgParallel)
            {
                Console.WriteLine($"  ✓ Sequential OCC is FASTER by {avgParallel - avgSequential}ms ({((double)(avgParallel - avgSequential) / avgParallel * 100):F1}%)");
            }
            else
            {
                Console.WriteLine($"  ✗ Parallel OCC is faster by {avgSequential - avgParallel}ms ({((double)(avgSequential - avgParallel) / avgSequential * 100):F1}%)");
            }
        }
        else
        {
            Console.WriteLine("  ⚠ Benchmark failed to complete");
        }
        
        Console.WriteLine();
    }

    private async Task ExecuteSequentialOCC()
    {
        _completedTransactions = 0;
        
        // Execute transactions sequentially
        foreach (var query in _queries)
        {
            try
            {
                var tx = _occExecutor.OCCExecuteSingle(query);
                ProcessTransactionResult(tx);
                _completedTransactions++;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"    Sequential error: {ex.Message}");
            }
        }
    }

    private async Task ExecuteParallelOCC()
    {
        _completedTransactions = 0;
        
        // Execute transactions in parallel
        var tasks = new List<Task>();
        
        foreach (var query in _queries)
        {
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var tx = _occExecutor.OCCExecuteSingle(query);
                    ProcessTransactionResult(tx);
                    Interlocked.Increment(ref _completedTransactions);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"    Parallel error: {ex.Message}");
                }
            }));
        }
        
        await Task.WhenAll(tasks);
    }

    private void ProcessTransactionResult(MinervaTx tx)
    {
        // Heavy processing on the transaction result to simulate workload
        if (tx != null && tx.Status == TransactionStatus.LocalCompleted && !string.IsNullOrEmpty(tx.Result))
        {
            // Make 10 copies of the result string to simulate heavy processing
            var processedResult = tx.Result;
            for (int i = 0; i < 10; i++)
            {
                processedResult = new string(processedResult.ToCharArray()); // Create new string copy
                // Do some string manipulation to make it heavier
                processedResult = processedResult.Replace("OK", "PROCESSED");
                processedResult = processedResult.ToUpper().ToLower(); // More processing
            }
            
            // Store the processed result length (to prevent optimization)
            _ = processedResult.Length;
        }
    }

    private void Setup()
    {
        _storage = new PersistentStorage();
        _queryParser = new BenchmarkQueryParser();
        _occExecutor = new MinervaTxOCCExecutor(_queryParser, _storage);
        _occExecutor.SetNewTempStates(new List<string> { "benchmark" }); // Initialize temp state
    }

    private void CreateBenchmarkQueries(int transactionCount)
    {
        _queries = new List<ClientRequest>();
        var random = new Random(42); // Fixed seed for reproducible results

        for (int i = 0; i < transactionCount; i++)
        {
            var query = CreateYCSBQuery(random, i, transactionCount);
            _queries.Add(query);
        }
    }

    private ClientRequest CreateYCSBQuery(Random random, int txId, int transactionCount)
    {
        var query = new ClientRequest
        {
            Type = QueryType.Ycsb,
            SeqId = (uint)txId,
            KVCmds = new List<KV>()
        };

        // Create heavy YCSB operations - 10 reads and 10 writes per transaction
        var keySpaceSize = Math.Max(10, transactionCount / 2); // ~50% contention
        
        // 10 read operations
        for (int j = 0; j < 10; j++) 
        {
            var keyId = random.Next(0, keySpaceSize);
            var key = GenerateKey(keyId); // 10-character key
            query.KVCmds.Add(new KV
            {
                Type = OpType.Get,
                Key = key,
                Value = "" // No value needed for reads
            });
        }
        
        // 10 write operations  
        for (int j = 0; j < 10; j++)
        {
            var keyId = random.Next(0, keySpaceSize);
            var key = GenerateKey(keyId); // 10-character key
            var value = GenerateValue(txId, j); // 1000-character value
            query.KVCmds.Add(new KV
            {
                Type = OpType.Set,
                Key = key,
                Value = value
            });
        }

        return query;
    }

    private string GenerateKey(int keyId)
    {
        // Generate 10-character key
        return $"key_{keyId:D6}"; // This gives us "key_000001" format (10 chars)
    }

    private string GenerateValue(int txId, int opId)
    {
        // Generate 1000-character value
        var baseValue = $"value_tx{txId}_op{opId}_";
        var padding = new string('x', 1000 - baseValue.Length);
        return baseValue + padding;
    }

    private void Cleanup()
    {
        // Nothing to cleanup for PersistentStorage in this simple case
    }
}