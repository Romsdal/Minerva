using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Minerva.Grpc_Client;

public class BenchmarkResult
{
    public ConcurrentBag<TxInfo> AllTxs { get; } = [];
    public int SentTx = 0;
    public int ExcutedTx = 0;
    public int LoadTx = 0;
    public int FailedTx = 0;
    public int IncompleteTx = 0;

    public void ParseAndPrintResults()
    {
        var allTransactions = AllTxs.ToList();
        var results = new ConcurrentBag<(TxType type, string output)>();

        // Process each transaction type concurrently
        Parallel.ForEach(Enum.GetValues<TxType>(), type =>
        {
            var output = ProcessTransactionType(type, allTransactions);
            results.Add((type, output));
            if (type == TxType.YCSB_LD || type == TxType.TPCC_LD)
            {

                LoadTx += allTransactions.Count(tx => tx.Type == type);
            }
        });


        Console.WriteLine("===================================================");
        Console.WriteLine("Benchmark Results:");
        Console.WriteLine("Sent Transactions: {0}", SentTx);
        Console.WriteLine("Excuted Transactions: {0}", ExcutedTx);
        Console.WriteLine("Load Transactions: {0}", LoadTx);
        Console.WriteLine("Incomplete Transactions (no response): {0}", IncompleteTx);
        Console.WriteLine("Failed Transactions: {0}", FailedTx);
        // Print results in the original enum order
        foreach (TxType type in Enum.GetValues<TxType>().Reverse())
        {
            var result = results.FirstOrDefault(r => r.type == type);
            if (!string.IsNullOrEmpty(result.output))
            {
                Console.WriteLine(result.output);
            }
        }

        // // sort all tx by sequence id
        // var sortedAllTx = allTransactions.OrderBy(tx => tx.Query.SeqId).ToList();
        // // print (seq, executed)
        // Console.WriteLine("All Transactions (SeqId, time):");
        // foreach (var tx in sortedAllTx)
        // {
        //     if (tx.Result.Executed)
        //     {
        //         Console.WriteLine($"{tx.Query.SeqId}, {(tx.EndTime - tx.StartTime).TotalMilliseconds} ms");
        //     }
        //     else
        //     {
        //         Console.WriteLine($"{tx.Query.SeqId}, Failed");
        //     }
        // }
        
        
    }

    private string ProcessTransactionType(TxType type, List<TxInfo> allTransactions)
    {
        // Filter transactions by type - 'All' includes all transactions
        var filteredTxs = type == TxType.All
            ? allTransactions.Where(tx => tx.Type != TxType.YCSB_LD && tx.Type != TxType.TPCC_LD).ToList()
            : allTransactions.Where(tx => tx.Type == type).ToList();

        int numAll = filteredTxs.Count;
        filteredTxs = filteredTxs.Where(tx => tx.Result.Executed).ToList();

        if (type == TxType.All)
        {   
            ExcutedTx += filteredTxs.Count;
            IncompleteTx += numAll - filteredTxs.Count;
        }

        if (!filteredTxs.Any())
        {
            return "";
        }

        var output = new System.Text.StringBuilder();
        
        // Calculate basic metrics
        int totalTxs = filteredTxs.Count;

        output.AppendLine($"Results for Type {type}:");
        output.AppendLine($"Total Txs: {totalTxs}");

        // Calculate overall throughput for TxType.All
        if (type == TxType.All && filteredTxs.Any())
        {
            var firstSentTime = filteredTxs.Min(tx => tx.StartTime);
            var lastReceivedTime = filteredTxs.Max(tx => tx.EndTime);
            var totalDuration = (lastReceivedTime - firstSentTime).TotalSeconds;
            var overallThroughput = totalDuration > 0 ? totalTxs / totalDuration : 0;
            output.AppendLine($"Overall Throughput: {overallThroughput:F2} txs/sec (from first sent to last received)");
        }

        // Calculate throughput over time (5 second intervals)
        var throughputOverTime = CalculateThroughputOverTime(filteredTxs);
        output.AppendLine($"Throughput over time (1s intervals): [{string.Join(", ", throughputOverTime)}] txs/sec, Max: {(throughputOverTime.Any() ? throughputOverTime.Max() : 0)} txs/sec");        // Calculate latency statistics
        var latencies = filteredTxs
            .Select(tx => (uint)(tx.EndTime - tx.StartTime).TotalMilliseconds)
            .ToList();

        CalculateLatencyStats(latencies, out double average, out double median, out double stdv, out double percentile99, out double percentile95);
        output.AppendLine($"Latency Stats:");
        output.AppendLine($"avg: {average:F2} ms");
        output.AppendLine($"med: {median:F2} ms");
        output.AppendLine($"stdv: {stdv:F2} ms");
        output.AppendLine($"p99: {percentile99:F2} ms");
        output.AppendLine($"p95: {percentile95:F2} ms");
        output.AppendLine($"===================================================");
        
        return output.ToString();
    }

    private static List<int> CalculateThroughputOverTime(List<TxInfo> transactions)
    {
        if (!transactions.Any())
            return new List<int>();

        // Get the time range
        var minTime = transactions.Min(tx => tx.EndTime);
        var maxTime = transactions.Max(tx => tx.EndTime);
        
        // Calculate duration in 5 second intervals
        var totalDurationSeconds = (maxTime - minTime).TotalSeconds;
        if (totalDurationSeconds < 1)
            totalDurationSeconds = 1;

        var sampleInterval = 1.0; // 1 second
        var totalSamples = (int)Math.Ceiling(totalDurationSeconds / sampleInterval);

        var throughputPerSeconds = new List<int>();
        
        // Count transactions completed in each 1-second window
        for (int sample = 0; sample < totalSamples; sample++)
        {
            var bucketStart = minTime.AddSeconds(sample * sampleInterval);
            var bucketEnd = minTime.AddSeconds((sample + 1) * sampleInterval);
            
            var txsInBucket = transactions.Count(tx => tx.EndTime >= bucketStart && tx.EndTime < bucketEnd);
            
            // Convert to transactions per second (divide by 5 since each bucket is 5s)
            var txsPerSecond = (int)Math.Round(txsInBucket / sampleInterval);
            throughputPerSeconds.Add(txsPerSecond);
        }

        return throughputPerSeconds;
    }



    private static void CalculateLatencyStats(List<uint> input, out double average, out double median, out double stdv, out double percentile99, out double percentile95)
    {
        double a = input.Average(x => x);
        double m = input.OrderBy(x => x).ElementAt(input.Count / 2);
        double s = CalculateStandardDeviation([.. input.Select(x => (double)x)]);
        double p99 = input.OrderBy(x => x).ElementAt((int)(input.Count * 0.99));
        double p95 = input.OrderBy(x => x).ElementAt((int)(input.Count * 0.95));

        average = a;
        median = m;
        stdv = s;
        percentile99 = p99;
        percentile95 = p95;
    }

    static double CalculateStandardDeviation(List<double> data)
    {
        // Calculate the mean (average) of the data
        double mean = data.Average();

        // Calculate the sum of squared differences from the mean
        double sumSquaredDifferences = data.Sum(value => Math.Pow(value - mean, 2));

        // Calculate the variance
        double variance = sumSquaredDifferences / data.Count;

        // Calculate the standard deviation as the square root of the variance
        double standardDeviation = Math.Sqrt(variance);

        return standardDeviation;
    }

}