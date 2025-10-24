using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;
using Minerva.DB_Server.MinervaLog;

namespace DeterExecTest;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Run DeterministicExecutor benchmark
        var deterministicBenchmark = new DeterministicExecutorBenchmark();
        await deterministicBenchmark.RunBenchmark();
        
        // Run OCC benchmark
        var occBenchmark = new OCCBenchmark();
        await occBenchmark.RunBenchmark();
    }
}

public class DeterministicExecutorBenchmark
{
    private DeterministicExecutor _executor = null!;
    private PersistentStorage _storage = null!;
    private BenchmarkQueryParser _queryParser = null!;
    private PriorityQueue<TransactionRecord, int>[] _transactions = null!;
    private int[] _replicaPriority = null!;
    private int _completedTransactions;

    public async Task RunBenchmark()
    {
        Setup();
        
        Console.WriteLine("DeterministicExecutor Performance Benchmark");
        Console.WriteLine("===========================================");
        Console.WriteLine("Testing when sequentialRun is faster than parallel execution\n");
        
        var transactionCounts = new int[] { 10, 50, 100, 200, 400, 800 }; // Higher transaction counts
        
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
        
        // Test sequential execution
        var sequentialTimes = new List<long>();
        var parallelTimes = new List<long>();
        
        // Run multiple iterations for more accurate results (reduced for higher tx counts)
        var iterations = transactionCount > 100 ? 2 : 3;
        for (int iteration = 0; iteration < iterations; iteration++)
        {
            Console.WriteLine($"  Iteration {iteration + 1}/{iterations}...");
            
            try
            {
                // Sequential run
                CreateBenchmarkTransactions(transactionCount);
                var sw = Stopwatch.StartNew();
                await _executor.DeterministicExecutionAsync(_transactions, _replicaPriority, TxCompleteHandler, 1, sequentialRun: true);
                sw.Stop();
                sequentialTimes.Add(sw.ElapsedMilliseconds);
                Console.WriteLine($"    Sequential: {sw.ElapsedMilliseconds}ms");
                
                // Parallel run with timeout (much longer timeout for heavy transactions)
                CreateBenchmarkTransactions(transactionCount);
                sw.Restart();
                
                var parallelTask = _executor.DeterministicExecutionAsync(_transactions, _replicaPriority, TxCompleteHandler, 1, sequentialRun: false);
                var timeoutMs = Math.Max(30000, transactionCount * 200); // Much longer timeout for heavy operations
                var timeoutTask = Task.Delay(timeoutMs); 
                
                var completedTask = await Task.WhenAny(parallelTask, timeoutTask);
                
                if (completedTask == timeoutTask)
                {
                    Console.WriteLine($"    Parallel: TIMEOUT (>{timeoutMs}ms) - likely deadlock or too slow");
                    parallelTimes.Add(timeoutMs); // Use timeout value as penalty
                }
                else
                {
                    sw.Stop();
                    parallelTimes.Add(sw.ElapsedMilliseconds);
                    Console.WriteLine($"    Parallel: {sw.ElapsedMilliseconds}ms");
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
            
            Console.WriteLine($"  Sequential Average: {avgSequential}ms");
            Console.WriteLine($"  Parallel Average:   {avgParallel}ms");
            
            if (avgSequential < avgParallel)
            {
                Console.WriteLine($"  ✓ Sequential is FASTER by {avgParallel - avgSequential}ms ({((double)(avgParallel - avgSequential) / avgParallel * 100):F1}%)");
            }
            else
            {
                Console.WriteLine($"  ✗ Parallel is faster by {avgSequential - avgParallel}ms ({((double)(avgSequential - avgParallel) / avgSequential * 100):F1}%)");
            }
        }
        else
        {
            Console.WriteLine("  ⚠ Benchmark failed to complete");
        }
        
        Console.WriteLine();
    }

    private void Setup()
    {
        _storage = new PersistentStorage();
        _queryParser = new BenchmarkQueryParser();
        _executor = new DeterministicExecutor(_queryParser, _storage);
        
        _replicaPriority = new int[] { 0 };
        _transactions = new PriorityQueue<TransactionRecord, int>[1];
    }

    private void CreateBenchmarkTransactions(int transactionCount)
    {
        _transactions[0] = new PriorityQueue<TransactionRecord, int>();
        var random = new Random(42); // Fixed seed for reproducible results

        for (int i = 0; i < transactionCount; i++)
        {
            var transaction = new TransactionRecord
            {
                Tid = i + 1,
                Query = CreateYCSBQuery(random, i),
                WriteSet = CreateWriteSet(random, i, transactionCount),
                ReadSet = CreateReadSet(random, i, transactionCount)
            };

            _transactions[0].Enqueue(transaction, i);
        }
    }

    private ClientRequest CreateYCSBQuery(Random random, int txId)
    {
        var query = new ClientRequest
        {
            Type = QueryType.Ycsb,
            SeqId = (uint)txId,
            KVCmds = new List<KV>()
        };

        // Create heavy YCSB operations - 10 reads and 10 writes per transaction
        var keySpaceSize = Math.Max(10, txId / 2); // ~50% contention
        
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

    private WriteSetStore CreateWriteSet(Random random, int txId, int transactionCount)
    {
        var writeSet = new WriteSetStore();
        
        // Create heavy write set with 10-char keys and 1000-char values
        var keySpaceSize = Math.Max(10, transactionCount / 2); // ~50% contention
        for (int j = 0; j < 10; j++) // 10 write operations
        {
            var keyId = random.Next(0, keySpaceSize);
            var key = GenerateKey(keyId);
            var value = GenerateValue(txId, j);
            writeSet.YCSBWriteSet[key] = value;
        }

        return writeSet;
    }

    private ReadSetStore CreateReadSet(Random random, int txId, int transactionCount)
    {
        var readSet = new ReadSetStore();
        
        // Create heavy read set with 10-char keys
        var keySpaceSize = Math.Max(10, transactionCount / 2); // ~50% contention
        for (int j = 0; j < 10; j++) // 10 read operations
        {
            var keyId = random.Next(0, keySpaceSize);
            var key = GenerateKey(keyId);
            readSet.YCSBReadKeys.Add(key);
        }

        return readSet;
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

    private void TxCompleteHandler(int tid, int sourceReplicaId, bool executed = false, string? result = null)
    {
        // Heavy processing on the transaction result to simulate workload
        if (executed && result != null)
        {
            // Make 10 copies of the result string to simulate heavy processing
            var processedResult = result;
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
        
        Interlocked.Increment(ref _completedTransactions);
    }

    private void Cleanup()
    {
        // Nothing to cleanup for PersistentStorage in this simple case
    }
}
