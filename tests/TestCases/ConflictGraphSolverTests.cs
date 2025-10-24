using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Minerva.DB_Server.ConflictResolver;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;

namespace TestCases;

public class ConflictGraphSolverTests : IDisposable
{
    private readonly PersistentStorage _persistentStorage;
    private readonly ConflictGraphSolver _solver;

    public ConflictGraphSolverTests()
    {
        _persistentStorage = new PersistentStorage();
        _solver = new ConflictGraphSolver(_persistentStorage);
    }

    public void Dispose()
    {
        // Cleanup resources if needed
        GC.SuppressFinalize(this);
    }

    #region Helper Methods

    /// <summary>
    /// Creates a simple transaction record for testing
    /// </summary>
    private TransactionRecord CreateTransactionRecord(int tid, List<int>? prevTids = null, 
        List<string>? readKeys = null, Dictionary<string, string>? writeKeys = null)
    {
        prevTids ??= new List<int>();
        readKeys ??= new List<string>();
        writeKeys ??= new Dictionary<string, string>();

        var writeSetStore = new WriteSetStore();
        foreach (var kvp in writeKeys)
        {
            writeSetStore.YCSBWriteSet[kvp.Key] = kvp.Value;
        }

        var readSetStore = new ReadSetStore();
        readSetStore.YCSBReadKeys.AddRange(readKeys);

        var keyAccessedFromSnapshot = new KeyAccessedFromSnapshotStore();
        foreach (var key in readKeys)
        {
            keyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot[key] = 0; // Default snapshot
        }

        return new TransactionRecord
        {
            Tid = tid,
            PrevTids = prevTids,
            Query = new ClientRequest { Type = QueryType.Ycsb, SeqId = (uint)tid },
            WriteSet = writeSetStore,
            ReadSet = readSetStore,
            KeyAccessedFromSnapshot = keyAccessedFromSnapshot
        };
    }

    /// <summary>
    /// Creates a batch with specified transactions
    /// </summary>
    private Batch CreateBatch(int batchId, int sourceReplicaId, List<TransactionRecord> transactions)
    {
        var batch = new Batch(batchId, sourceReplicaId);
        batch.Transactions.AddRange(transactions);
        return batch;
    }

    /// <summary>
    /// Sets up initial data in persistent storage for testing staleness detection
    /// </summary>
    private void SetupPersistentData(string key, string value, int cid)
    {
        var entry = new PersistentStorage.PersistEntry<string>(value, cid);
        _persistentStorage.YCSBStore[key] = entry;
    }

    /// <summary>
    /// Extracts results and stales from globalEpochBatches based on ConflictStatus
    /// </summary>
    private (List<TransactionRecord>[], List<TransactionRecord>[]) GetResultsAndStales(List<Batch>[] globalEpochBatches, int numReplicas)
    {
        var results = new List<TransactionRecord>[numReplicas];
        var stales = new List<TransactionRecord>[numReplicas];
        
        for (int i = 0; i < numReplicas; i++)
        {
            results[i] = new List<TransactionRecord>();
            stales[i] = new List<TransactionRecord>();
            
            if (globalEpochBatches[i] != null)
            {
                foreach (var batch in globalEpochBatches[i])
                {
                    foreach (var tx in batch.Transactions)
                    {
                        if (tx.ConflictStatus == TxConflict.None)
                        {
                            results[i].Add(tx);
                        }
                        else if (tx.ConflictStatus == TxConflict.Stale)
                        {
                            stales[i].Add(tx);
                        }
                    }
                }
            }
        }
        
        return (results, stales);
    }

    #endregion

    #region Basic Functionality Tests

    [Fact]
    public void FindConflicts_EmptyBatches_ReturnsEmptyResults()
    {
        // Arrange
        var globalEpochBatches = new List<Batch>[2];
        globalEpochBatches[0] = new List<Batch>();
        globalEpochBatches[1] = new List<Batch>();

        // Act
        _solver.FindConflicts(globalEpochBatches, 2, exact: true);

        // Assert
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 2);
        Assert.Equal(2, results.Length);
        Assert.Equal(2, stales.Length);
        Assert.Empty(results[0]);
        Assert.Empty(results[1]);
        Assert.Empty(stales[0]);
        Assert.Empty(stales[1]);
    }

    [Fact]
    public void FindConflicts_SingleTransactionNoDependencies_ReturnsTransaction()
    {
        // Arrange
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["key1"] = "value1" });
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        
        var globalEpochBatches = new List<Batch>[1];
        globalEpochBatches[0] = new List<Batch> { batch1 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 1, exact: true);

        // Assert
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 1);
        Assert.Single(results[0]);
        Assert.Equal(1, results[0][0].Tid);
        Assert.Empty(stales[0]);
    }

    #endregion

    #region Conflict Detection Tests

    [Fact]
    public void FindConflicts_WriteWriteConflict_ReturnsConflictFreeSet()
    {
        // Arrange - Two transactions writing to the same key from different replicas
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["shared_key"] = "value1" });
        var tx2 = CreateTransactionRecord(2, writeKeys: new Dictionary<string, string> { ["shared_key"] = "value2" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        var batch2 = CreateBatch(2, 1, new List<TransactionRecord> { tx2 });
        
        var globalEpochBatches = new List<Batch>[2];
        globalEpochBatches[0] = new List<Batch> { batch1 };
        globalEpochBatches[1] = new List<Batch> { batch2 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 2, exact: true);

        // Assert - Only one transaction should be selected due to conflict
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 2);
        var totalTransactions = results[0].Count + results[1].Count;
        Assert.Equal(1, totalTransactions);
        Assert.Empty(stales[0]);
        Assert.Empty(stales[1]);
    }

    [Fact]
    public void FindConflicts_ReadWriteConflict_ReturnsConflictFreeSet()
    {
        // Arrange - One transaction reads, another writes to the same key
        var tx1 = CreateTransactionRecord(1, readKeys: new List<string> { "shared_key" });
        var tx2 = CreateTransactionRecord(2, writeKeys: new Dictionary<string, string> { ["shared_key"] = "value2" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        var batch2 = CreateBatch(2, 1, new List<TransactionRecord> { tx2 });
        
        var globalEpochBatches = new List<Batch>[2];
        globalEpochBatches[0] = new List<Batch> { batch1 };
        globalEpochBatches[1] = new List<Batch> { batch2 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 2, exact: true);

        // Assert - Only one transaction should be selected due to conflict
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 2);
        var totalTransactions = results[0].Count + results[1].Count;
        Assert.Equal(1, totalTransactions);
    }

    [Fact]
    public void FindConflicts_NoConflicts_ReturnsAllTransactions()
    {
        // Arrange - Independent transactions with different keys
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["key1"] = "value1" });
        var tx2 = CreateTransactionRecord(2, writeKeys: new Dictionary<string, string> { ["key2"] = "value2" });
        var tx3 = CreateTransactionRecord(3, readKeys: new List<string> { "key3" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        var batch2 = CreateBatch(2, 1, new List<TransactionRecord> { tx2 });
        var batch3 = CreateBatch(3, 0, new List<TransactionRecord> { tx3 });
        
        var globalEpochBatches = new List<Batch>[2];
        globalEpochBatches[0] = new List<Batch> { batch1, batch3 };
        globalEpochBatches[1] = new List<Batch> { batch2 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 2, exact: true);

        // Assert - All transactions should be included (no conflicts)
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 2);
        var totalTransactions = results[0].Count + results[1].Count;
        Assert.Equal(3, totalTransactions);
    }

    #endregion

    #region Transaction Chain Tests

    [Fact]
    public void FindConflicts_DependentTransactionChain_TreatedAsUnit()
    {
        // Arrange - Chain of dependent transactions within same replica
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["key1"] = "value1" });
        var tx2 = CreateTransactionRecord(2, prevTids: new List<int> { 1 }, 
                                        readKeys: new List<string> { "key1" },
                                        writeKeys: new Dictionary<string, string> { ["key2"] = "value2" });
        var tx3 = CreateTransactionRecord(3, prevTids: new List<int> { 2 },
                                        readKeys: new List<string> { "key2" },
                                        writeKeys: new Dictionary<string, string> { ["key3"] = "value3" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1, tx2, tx3 });
        
        var globalEpochBatches = new List<Batch>[1];
        globalEpochBatches[0] = new List<Batch> { batch1 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 1, exact: true);

        // Assert - All transactions in chain should be returned together
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 1);
        Assert.Equal(3, results[0].Count);
        var tids = results[0].Select(tx => tx.Tid).OrderBy(x => x).ToList();
        Assert.Equal(new List<int> { 1, 2, 3 }, tids);
    }

    [Fact]
    public void FindConflicts_ChainWithConflict_EntireChainAffected()
    {
        // Arrange - Chain of transactions with external conflict
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["shared_key"] = "value1" });
        var tx2 = CreateTransactionRecord(2, prevTids: new List<int> { 1 },
                                        readKeys: new List<string> { "shared_key" },
                                        writeKeys: new Dictionary<string, string> { ["key2"] = "value2" });
        
        var tx3 = CreateTransactionRecord(3, writeKeys: new Dictionary<string, string> { ["shared_key"] = "value3" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1, tx2 });
        var batch2 = CreateBatch(2, 1, new List<TransactionRecord> { tx3 });
        
        var globalEpochBatches = new List<Batch>[2];
        globalEpochBatches[0] = new List<Batch> { batch1 };
        globalEpochBatches[1] = new List<Batch> { batch2 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 2, exact: true);

        // Assert - Only one chain should be selected
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 2);
        var totalTransactions = results[0].Count + results[1].Count;
        Assert.True(totalTransactions == 1 || totalTransactions == 2); // Either single tx or the chain
    }

    #endregion

    #region Staleness Detection Tests

    [Fact]
    public void FindConflicts_StaleTransaction_MovedToStalesList()
    {
        // Arrange - Transaction reading outdated data
        SetupPersistentData("stale_key", "old_value", 5); // Data updated at commit 5
        
        var tx1 = CreateTransactionRecord(1, readKeys: new List<string> { "stale_key" });
        // Simulate this transaction read from an older snapshot (commit 3)
        tx1.KeyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot["stale_key"] = 3;
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        
        var globalEpochBatches = new List<Batch>[1];
        globalEpochBatches[0] = new List<Batch> { batch1 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 1, exact: true);

        // Assert - Transaction should be marked as stale
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 1);
        Assert.Empty(results[0]);
        Assert.Single(stales[0]);
        Assert.Equal(1, stales[0][0].Tid);
    }

    [Fact]
    public void FindConflicts_FreshTransaction_IncludedInResults()
    {
        // Arrange - Transaction reading up-to-date data
        SetupPersistentData("fresh_key", "current_value", 3); // Data updated at commit 3
        
        var tx1 = CreateTransactionRecord(1, readKeys: new List<string> { "fresh_key" });
        // This transaction read from current or newer snapshot
        tx1.KeyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot["fresh_key"] = 3;
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        
        var globalEpochBatches = new List<Batch>[1];
        globalEpochBatches[0] = new List<Batch> { batch1 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 1, exact: true);

        // Assert - Transaction should be included in results
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 1);
        Assert.Single(results[0]);
        Assert.Empty(stales[0]);
        Assert.Equal(1, results[0][0].Tid);
    }

    [Fact]
    public void FindConflicts_ChainWithStaleTransaction_EntireChainStale()
    {
        // Arrange - Chain where one transaction is stale
        SetupPersistentData("stale_key", "old_value", 5);
        
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["key1"] = "value1" });
        var tx2 = CreateTransactionRecord(2, prevTids: new List<int> { 1 },
                                        readKeys: new List<string> { "stale_key" }); // This read is stale
        tx2.KeyAccessedFromSnapshot.YCSBKeyAccessedFromSnapshot["stale_key"] = 3; // Read from older snapshot
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1, tx2 });
        
        var globalEpochBatches = new List<Batch>[1];
        globalEpochBatches[0] = new List<Batch> { batch1 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 1, exact: true);

        // Assert - Entire chain should be marked as stale
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 1);
        Assert.Empty(results[0]);
        Assert.Equal(2, stales[0].Count);
    }

    #endregion

    #region Algorithm Comparison Tests

    [Fact]
    public void FindConflicts_ExactVsGreedy_BothReturnValidSolutions()
    {
        // Arrange - Complex conflict scenario
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["key1"] = "value1", ["key2"] = "value2" });
        var tx2 = CreateTransactionRecord(2, writeKeys: new Dictionary<string, string> { ["key1"] = "value1_alt" });
        var tx3 = CreateTransactionRecord(3, writeKeys: new Dictionary<string, string> { ["key2"] = "value2_alt" });
        var tx4 = CreateTransactionRecord(4, writeKeys: new Dictionary<string, string> { ["key3"] = "value3" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        var batch2 = CreateBatch(2, 1, new List<TransactionRecord> { tx2 });
        var batch3 = CreateBatch(3, 2, new List<TransactionRecord> { tx3 });
        var batch4 = CreateBatch(4, 0, new List<TransactionRecord> { tx4 });
        
        var globalEpochBatches = new List<Batch>[3];
        globalEpochBatches[0] = new List<Batch> { batch1, batch4 };
        globalEpochBatches[1] = new List<Batch> { batch2 };
        globalEpochBatches[2] = new List<Batch> { batch3 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 3, exact: true);
        var (exactResults, exactStales) = GetResultsAndStales(globalEpochBatches, 3);
        
        // Reset ConflictStatus for greedy run
        foreach (var replicaBatches in globalEpochBatches)
        {
            foreach (var batch in replicaBatches)
            {
                foreach (var tx in batch.Transactions)
                {
                    tx.ConflictStatus = TxConflict.Conflict;
                }
            }
        }
        
        _solver.FindConflicts(globalEpochBatches, 3, exact: false);
        var (greedyResults, greedyStales) = GetResultsAndStales(globalEpochBatches, 3);

        // Assert - Both should return conflict-free solutions
        VerifyConflictFree(exactResults);
        VerifyConflictFree(greedyResults);
        
        // Both should include at least the independent transaction (tx4)
        var exactTotal = exactResults.Sum(r => r.Count);
        var greedyTotal = greedyResults.Sum(r => r.Count);
        Assert.True(exactTotal >= 1);
        Assert.True(greedyTotal >= 1);
    }

    #endregion

    #region Edge Case Tests

    [Fact]
    public void FindConflicts_SingleReplicaMultipleBatches_HandledCorrectly()
    {
        // Arrange - Multiple batches from same replica
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["key1"] = "value1" });
        var tx2 = CreateTransactionRecord(2, writeKeys: new Dictionary<string, string> { ["key2"] = "value2" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        var batch2 = CreateBatch(2, 0, new List<TransactionRecord> { tx2 });
        
        var globalEpochBatches = new List<Batch>[1];
        globalEpochBatches[0] = new List<Batch> { batch1, batch2 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 1, exact: true);

        // Assert - Both transactions should be included (no conflicts)
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 1);
        Assert.Equal(2, results[0].Count);
        Assert.Empty(stales[0]);
    }

    [Fact]
    public void FindConflicts_TransactionWithSelfDependency_HandledGracefully()
    {
        // Arrange - Transaction that somehow references itself (edge case)
        var tx1 = CreateTransactionRecord(1, 
                                        prevTids: new List<int> { 1 }, // Self-reference
                                        writeKeys: new Dictionary<string, string> { ["key1"] = "value1" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1 });
        
        var globalEpochBatches = new List<Batch>[1];
        globalEpochBatches[0] = new List<Batch> { batch1 };

        // Act & Assert - Should not throw exception
        _solver.FindConflicts(globalEpochBatches, 1, exact: true);
        
        // Transaction should still be processed
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 1);
        Assert.True(results[0].Count + stales[0].Count == 1);
    }

    [Fact]
    public void FindConflicts_LargeNumberOfTransactions_PerformsReasonably()
    {
        // Arrange - Create many independent transactions
        var batches = new List<Batch>[1];
        batches[0] = new List<Batch>();
        
        var allTransactions = new List<TransactionRecord>();
        for (int i = 1; i <= 100; i++)
        {
            var tx = CreateTransactionRecord(i, writeKeys: new Dictionary<string, string> { [$"key_{i}"] = $"value_{i}" });
            allTransactions.Add(tx);
        }
        
        var batch = CreateBatch(1, 0, allTransactions);
        batches[0].Add(batch);

        // Act
        var startTime = DateTime.UtcNow;
        _solver.FindConflicts(batches, 1, exact: false); // Use greedy for performance
        var duration = DateTime.UtcNow - startTime;

        // Assert - Should complete in reasonable time and return all transactions
        var (results, stales) = GetResultsAndStales(batches, 1);
        Assert.True(duration.TotalSeconds < 10); // Should complete within 10 seconds
        Assert.Equal(100, results[0].Count);
        Assert.Empty(stales[0]);
    }

    #endregion

    #region Complex Scenario Tests

    [Fact]
    public void FindConflicts_ComplexMultiReplicaScenario_ReturnsValidSolution()
    {
        // Arrange - Realistic multi-replica scenario with various conflicts
        
        // Replica 0: Chain of 2 transactions
        var tx1 = CreateTransactionRecord(1, writeKeys: new Dictionary<string, string> { ["account_1"] = "1000" });
        var tx2 = CreateTransactionRecord(2, prevTids: new List<int> { 1 },
                                        readKeys: new List<string> { "account_1" },
                                        writeKeys: new Dictionary<string, string> { ["account_2"] = "500" });
        
        // Replica 1: Conflicting transaction
        var tx3 = CreateTransactionRecord(3, writeKeys: new Dictionary<string, string> { ["account_1"] = "2000" });
        
        // Replica 2: Independent transaction
        var tx4 = CreateTransactionRecord(4, writeKeys: new Dictionary<string, string> { ["account_3"] = "750" });
        
        // Replica 0: Another independent transaction  
        var tx5 = CreateTransactionRecord(5, writeKeys: new Dictionary<string, string> { ["account_4"] = "300" });
        
        var batch1 = CreateBatch(1, 0, new List<TransactionRecord> { tx1, tx2 });
        var batch2 = CreateBatch(2, 1, new List<TransactionRecord> { tx3 });
        var batch3 = CreateBatch(3, 2, new List<TransactionRecord> { tx4 });
        var batch4 = CreateBatch(4, 0, new List<TransactionRecord> { tx5 });
        
        var globalEpochBatches = new List<Batch>[3];
        globalEpochBatches[0] = new List<Batch> { batch1, batch4 };
        globalEpochBatches[1] = new List<Batch> { batch2 };
        globalEpochBatches[2] = new List<Batch> { batch3 };

        // Act
        _solver.FindConflicts(globalEpochBatches, 3, exact: true);

        // Assert
        var (results, stales) = GetResultsAndStales(globalEpochBatches, 3);
        VerifyConflictFree(results);
        
        // Should include independent transactions (tx4, tx5) and either the chain (tx1,tx2) or tx3
        var totalSelected = results.Sum(r => r.Count);
        Assert.True(totalSelected >= 2); // At minimum tx4 and tx5
        Assert.True(totalSelected <= 4); // At maximum all except one conflicting part
    }

    #endregion

    #region Helper Verification Methods

    /// <summary>
    /// Verifies that the selected transactions have no write-write or read-write conflicts
    /// </summary>
    private void VerifyConflictFree(List<TransactionRecord>[] results)
    {
        var allSelected = results.SelectMany(r => r).ToList();
        var writeKeys = new Dictionary<string, List<int>>();
        var readKeys = new Dictionary<string, List<int>>();
        
        // Collect all read and write operations
        foreach (var tx in allSelected)
        {
            // Collect writes
            foreach (var key in tx.WriteSet.YCSBWriteSet.Keys)
            {
                if (!writeKeys.ContainsKey(key))
                    writeKeys[key] = new List<int>();
                writeKeys[key].Add(tx.Tid);
            }
            
            // Collect reads
            foreach (var key in tx.ReadSet.YCSBReadKeys)
            {
                if (!readKeys.ContainsKey(key))
                    readKeys[key] = new List<int>();
                readKeys[key].Add(tx.Tid);
            }
        }
        
        // Check for write-write conflicts
        foreach (var kvp in writeKeys)
        {
            if (kvp.Value.Count > 1)
            {
                // Multiple writers to same key - check if they're from same replica
                var writersFromDifferentReplicas = kvp.Value
                    .Select(tid => allSelected.First(tx => tx.Tid == tid))
                    .GroupBy(tx => GetReplicaId(tx, results))
                    .Count() > 1;
                    
                Assert.False(writersFromDifferentReplicas, 
                    $"Write-write conflict detected on key {kvp.Key} between different replicas");
            }
        }
        
        // Check for read-write conflicts  
        foreach (var writeKvp in writeKeys)
        {
            if (readKeys.TryGetValue(writeKvp.Key, out var readers))
            {
                var writers = writeKvp.Value;
                foreach (var readerId in readers)
                {
                    foreach (var writerId in writers)
                    {
                        if (readerId != writerId)
                        {
                            var readerReplica = GetReplicaId(allSelected.First(tx => tx.Tid == readerId), results);
                            var writerReplica = GetReplicaId(allSelected.First(tx => tx.Tid == writerId), results);
                            
                            Assert.True(readerReplica == writerReplica,
                                $"Read-write conflict detected on key {writeKvp.Key} between different replicas");
                        }
                    }
                }
            }
        }
    }
    
    /// <summary>
    /// Gets the replica ID for a transaction based on which result array it appears in
    /// </summary>
    private int GetReplicaId(TransactionRecord tx, List<TransactionRecord>[] results)
    {
        for (int i = 0; i < results.Length; i++)
        {
            if (results[i].Any(t => t.Tid == tx.Tid))
                return i;
        }
        return -1; // Should never happen in valid test
    }

    #endregion
}