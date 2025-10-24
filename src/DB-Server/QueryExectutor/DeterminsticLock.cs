

using System.Collections.Concurrent;
using System.Collections.Generic;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Transactions;

namespace DB_Server.QueryExectutor;

public enum LockMode
{
    Unlocked,
    Read,
    Write
}

public struct LockRequest
{
    public LockRequest(TransactionRecord tx, LockMode mode)
    {
        Tx = tx;
        Mode = mode;
    }

    public TransactionRecord Tx { get; }
    public LockMode Mode { get; }
}



public class DeterminsticLockManager
{

    private struct LockTables
    {
        public readonly Dictionary<(int shard, string key), LinkedList<LockRequest>> YCSBLockTable = new();

        public readonly Dictionary<long, LinkedList<LockRequest>> WareHouseLockTable = new();
        public readonly Dictionary<(long DWID, long DID), LinkedList<LockRequest>> DistrictLockTable = new();
        public readonly Dictionary<(long DWID, long DID, long CID), LinkedList<LockRequest>> CustomerLockTable = new();
        public readonly Dictionary<long, LinkedList<LockRequest>> ItemLockTable = new();
        public readonly Dictionary<(long SWID, long SIID), LinkedList<LockRequest>> StockLockTable = new();
        public readonly Dictionary<(long HCID, long HDATE), LinkedList<LockRequest>> HistoryLockTable = new();
        public readonly Dictionary<(long NOWID, long NODID, long NOOID), LinkedList<LockRequest>> NewOrderLockTable = new();
        public readonly Dictionary<(long OWID, long ODID, long OOID), LinkedList<LockRequest>> OrderLockTable = new();
        public readonly Dictionary<(long OWID, long ODID, long OOID, long OLID), LinkedList<LockRequest>> OrderLineLockTable = new();

        public LockTables() { }
    }




    public readonly ConcurrentQueue<TransactionRecord> RreadyTxns = [];
    public readonly Dictionary<TransactionRecord, int> RtxnWaits = [];
    private readonly LockTables _lockTables = new();

    public DeterminsticLockManager()
    { }


    public int Lock(TransactionRecord tx)
    {
        var notAcquired = 0;

        GetWriteLock(tx, tx.WriteSet.YCSBWriteSet.Keys, _lockTables.YCSBLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.WarehouseWriteSet.Keys, _lockTables.WareHouseLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.DistrictWriteSet.Keys, _lockTables.DistrictLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.CustomerWriteSet.Keys, _lockTables.CustomerLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.ItemWriteSet.Keys, _lockTables.ItemLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.StockWriteSet.Keys, _lockTables.StockLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.HistoryWriteSet.Keys, _lockTables.HistoryLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.NewOrderWriteSet.Keys, _lockTables.NewOrderLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.OrderWriteSet.Keys, _lockTables.OrderLockTable, ref notAcquired);
        GetWriteLock(tx, tx.WriteSet.OrderLineWriteSet.Keys, _lockTables.OrderLineLockTable, ref notAcquired);

        GetReadLock(tx, tx.ReadSet.YCSBReadKeys, _lockTables.YCSBLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.WarehouseReadKeys, _lockTables.WareHouseLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.DistrictReadKeys, _lockTables.DistrictLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.CustomerReadKeys, _lockTables.CustomerLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.ItemReadKeys, _lockTables.ItemLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.StockReadKeys, _lockTables.StockLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.HistoryReadKeys, _lockTables.HistoryLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.NewOrderReadKeys, _lockTables.NewOrderLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.OrderReadKeys, _lockTables.OrderLockTable, ref notAcquired);
        GetReadLock(tx, tx.ReadSet.OrderLineReadKeys, _lockTables.OrderLineLockTable, ref notAcquired);


        if (notAcquired > 0)
        {
            RtxnWaits[tx] = notAcquired;
        }
        else
        {
            RreadyTxns.Enqueue(tx);
        }

        return 0;
    }

    private void GetWriteLock<Tkey>(TransactionRecord tx, ICollection<Tkey> keys, Dictionary<Tkey, LinkedList<LockRequest>> table, ref int notAcquired)
    {
        foreach (var key in keys)
        {
            var requests = LookupRequests(table, key);

            if (requests.Count == 0 || requests.Last.Value.Tx != tx)
            {
                requests.AddLast(new LockRequest(tx, LockMode.Write));
                if (requests.Count > 1)
                {
                    notAcquired++;
                }
            }
        }
    }

    public void GetReadLock<Tkey>(TransactionRecord tx, List<Tkey> readKeys, Dictionary<Tkey, LinkedList<LockRequest>> table, ref int notAcquired)
    {
        foreach (var key in readKeys)
        {
            var requests = LookupRequests(table, key);

            if (requests.Count == 0 || requests.Last.Value.Tx != tx)
            {
                requests.AddLast(new LockRequest(tx, LockMode.Read));

                for (var node = requests.First; node != null; node = node.Next)
                {
                    if (node.Value.Mode == LockMode.Write)
                    {
                        notAcquired++;
                        break;
                    }
                }
            }
        }
    }


    private LinkedList<LockRequest> LookupRequests<TKey>(Dictionary<TKey, LinkedList<LockRequest>> table, TKey key)
    {
        if (!table.TryGetValue(key, out LinkedList<LockRequest> value))
        {
            value = new LinkedList<LockRequest>();
            table[key] = value;
        }
        return value;
    }

    public void Release(TransactionRecord tx)
    {
        lock (this)
        {
            ReleaseLocks(tx, tx.WriteSet.YCSBWriteSet, tx.ReadSet.YCSBReadKeys, _lockTables.YCSBLockTable);
            ReleaseLocks(tx, tx.WriteSet.WarehouseWriteSet, tx.ReadSet.WarehouseReadKeys, _lockTables.WareHouseLockTable);
            ReleaseLocks(tx, tx.WriteSet.DistrictWriteSet, tx.ReadSet.DistrictReadKeys, _lockTables.DistrictLockTable);
            ReleaseLocks(tx, tx.WriteSet.CustomerWriteSet, tx.ReadSet.CustomerReadKeys, _lockTables.CustomerLockTable);
            ReleaseLocks(tx, tx.WriteSet.ItemWriteSet, tx.ReadSet.ItemReadKeys, _lockTables.ItemLockTable);
            ReleaseLocks(tx, tx.WriteSet.StockWriteSet, tx.ReadSet.StockReadKeys, _lockTables.StockLockTable);
            ReleaseLocks(tx, tx.WriteSet.HistoryWriteSet, tx.ReadSet.HistoryReadKeys, _lockTables.HistoryLockTable);
            ReleaseLocks(tx, tx.WriteSet.NewOrderWriteSet, tx.ReadSet.NewOrderReadKeys, _lockTables.NewOrderLockTable);
            ReleaseLocks(tx, tx.WriteSet.OrderWriteSet, tx.ReadSet.OrderReadKeys, _lockTables.OrderLockTable);
            ReleaseLocks(tx, tx.WriteSet.OrderLineWriteSet, tx.ReadSet.OrderLineReadKeys, _lockTables.OrderLineLockTable);
        }

    }

    
    public void ReleaseLocks<Tkey, TValue>(TransactionRecord tx, Dictionary<Tkey, TValue> writeSet, List<Tkey> readSet, Dictionary<Tkey, LinkedList<LockRequest>> table)
    {
        foreach (var key in writeSet.Keys)
        {
            ReleaseKey(key, tx, table);
        }

        foreach (var key in readSet)
        {
            ReleaseKey(key, tx, table);
        }
    }

    private void ReleaseKey<TKey>(TKey key, TransactionRecord tx, Dictionary<TKey, LinkedList<LockRequest>> table)
    {
        if (!table.TryGetValue(key, out var requests))
        {
            return;
        }

        var node = requests.First;
        var writeBeforeTarget = false;

        while (node != null && node.Value.Tx != tx)
        {
            if (node.Value.Mode == LockMode.Write)
            {
                writeBeforeTarget = true;
            }

            node = node.Next;
        }

        if (node == null)
        {
            return;
        }

        
        var target = node;
        node = node.Next;

        if (node != null)
        {
            var newOwners = new List<TransactionRecord>();

            if (target == requests.First &&
                (target.Value.Mode == LockMode.Write ||
                    (target.Value.Mode == LockMode.Read && node.Value.Mode == LockMode.Write)))
            {
                if (node.Value.Mode == LockMode.Write)
                {
                    newOwners.Add(node.Value.Tx);
                    node = node.Next;
                }

                while (node != null && node.Value.Mode == LockMode.Read)
                {
                    newOwners.Add(node.Value.Tx);
                    node = node.Next;
                }
            }
            else if (!writeBeforeTarget && target.Value.Mode == LockMode.Write && node.Value.Mode == LockMode.Read)
            {
                while (node != null && node.Value.Mode == LockMode.Read)
                {
                    newOwners.Add(node.Value.Tx);
                    node = node.Next;
                }
            }

            foreach (var owner in newOwners)
            {
                if (RtxnWaits.TryGetValue(owner, out var outstanding))
                {
                    outstanding -= 1;
                    if (outstanding <= 0)
                    {
                        RreadyTxns.Enqueue(owner);
                        RtxnWaits.Remove(owner, out _);
                    }
                    else
                    {
                        RtxnWaits[owner] = outstanding;
                    }
                }
            }
        }

        requests.Remove(target);

        if (requests.Count == 0)
        {
            table.Remove(key);
        }


    }


}