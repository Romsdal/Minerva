using System;
using System.Collections.Generic;
using System.IO;
using Minerva.DB_Server;
using Minerva.DB_Server.ConflictResolver;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.MinervaLog;

namespace ConflictGraphBenchmark;

/// <summary>
/// Simple test to verify timing log capture is working
/// </summary>
public static class TestTimingCapture
{
    public static void RunTest()
    {
        Console.WriteLine("Testing ConflictGraphSolver timing log capture...");
        
        var persistentStorage = new PersistentStorage();
        var solver = new ConflictGraphSolver(persistentStorage);
        
        // Set up log capturing
        var originalOut = Console.Out;
        var logCapture = new StringWriter();
        
        try
        {
            // Enable debug logging
            LoggerManager.LoggingLevel = "0"; // Debug level
            LoggerManager.NodeId = 0;
            Console.SetOut(logCapture);
            
            // Create a simple test scenario
            var batches = new List<Batch>[3];
            for (int i = 0; i < 3; i++)
            {
                batches[i] = new List<Batch>();
            }
            
            // Add a simple transaction
            var writeSetStore = new WriteSetStore();
            writeSetStore.YCSBWriteSet["test_key"] = "test_value";
            
            var tx = new TransactionRecord
            {
                Tid = 1,
                PrevTids = new List<int>(),
                Query = new ClientRequest { Type = QueryType.Ycsb, SeqId = 1 },
                WriteSet = writeSetStore,
                ReadSet = new ReadSetStore(),
                KeyAccessedFromSnapshot = new KeyAccessedFromSnapshotStore()
            };
            
            var batch = new Batch(1, 0);
            batch.Transactions.Add(tx);
            batches[0].Add(batch);
            
            // Run the solver
            var result = solver.FindConflicts(batches, 3, exact: true);
            
            // Capture and analyze logs
            var logs = logCapture.ToString();
            Console.SetOut(originalOut); // Restore output to see results
            
            Console.WriteLine("Captured logs:");
            Console.WriteLine(logs);
            Console.WriteLine();
            
            var timings = ConflictGraphTimings.ParseFromLogs(logs);
            Console.WriteLine("Parsed timings:");
            Console.WriteLine($"  Construct Chains: {timings.ConstructChainsMs}ms");
            Console.WriteLine($"  Check Stale: {timings.CheckStaleMs}ms");
            Console.WriteLine($"  Track RW Sets: {timings.TrackRWSetMs}ms");
            Console.WriteLine($"  Construct Graph: {timings.ConstructGraphMs}ms");
            Console.WriteLine($"  Solve MWIS: {timings.SolveMWISMs}ms");
            Console.WriteLine($"  Compile Results: {timings.CompileResultsMs}ms");
            Console.WriteLine($"  Total: {timings.TotalMs}ms");
            
        }
        finally
        {
            Console.SetOut(originalOut);
            LoggerManager.LoggingLevel = "-1"; // Disable logging
            logCapture.Dispose();
        }
    }
}