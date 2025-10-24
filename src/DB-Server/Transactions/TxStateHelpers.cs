using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;
using static Minerva.DB_Server.Storage.PersistentStorage;

namespace Minerva.DB_Server.Transactions;


public static class TxStateHelpers
{
    public static void ApplyWriteSetToPersistentDB(WriteSetStore writeSet, PersistentStorage pdb, int currCid)
    {
        SaveToPersistedState(writeSet.YCSBWriteSet, pdb, Database.YCSB, currCid);
        SaveToPersistedState(writeSet.WarehouseWriteSet, pdb, Database.Warehouse, currCid);
        SaveToPersistedState(writeSet.DistrictWriteSet, pdb, Database.District, currCid);
        SaveToPersistedState(writeSet.CustomerWriteSet, pdb, Database.Customer, currCid);
        SaveToPersistedState(writeSet.ItemWriteSet, pdb, Database.Item, currCid);
        SaveToPersistedState(writeSet.StockWriteSet, pdb, Database.Stock, currCid);
        SaveToPersistedState(writeSet.HistoryWriteSet, pdb, Database.History, currCid);
        SaveToPersistedState(writeSet.NewOrderWriteSet, pdb, Database.NewOrder, currCid);
        SaveToPersistedState(writeSet.OrderWriteSet, pdb, Database.Order, currCid);
        SaveToPersistedState(writeSet.OrderLineWriteSet, pdb, Database.OrderLine, currCid);

        ConstructCustomerIndexByLastName(writeSet.CustomerWriteSet, pdb.CustomerIndexByLastName);
    }

    private static void SaveToPersistedState<TKey, TValue>(Dictionary<TKey, TValue> writeSet,
        PersistentStorage persistedDB,
        Database database,
        int currCid)
    {
        foreach (var (key, value) in writeSet)
        {
            // if we are doing
            if (value is TPCCItem tpccItem && tpccItem.IsPatch)
            {
                if (persistedDB.Get(database, key, out var existingEntry))
                {
                    var tpccExistingEntry = ((PersistEntry<TValue>)existingEntry).Value as TPCCItem;
                    tpccExistingEntry.Patch(tpccItem);
                    ((PersistEntry<TValue>)existingEntry).Cid = currCid;

                }
                else
                {
                    throw new InvalidOperationException("Cannot patch a non-existing entry.");
                }
            }
            else
            {
                if (persistedDB.Get(database, key, out var existingEntry))
                {
                    ((PersistEntry<TValue>)existingEntry).Value = value;
                    ((PersistEntry<TValue>)existingEntry).Cid = currCid;
                }
                else
                {
                    var val = new PersistEntry<TValue>(value, currCid);
                    persistedDB.Set(database, key, val);
                }
            }
        }
    }

    private static void ConstructCustomerIndexByLastName(Dictionary<(long CWID, long CDID, long CID), Customer> customerWriterSet, Dictionary<string, HashSet<(long CWID, long CDID, long CID)>> customerIndexByLastName)
    {
        foreach (var (key, customer) in customerWriterSet)
        {
            StringBuilder sb = new();
            sb.Append(customer.C_W_ID);
            sb.Append('-');
            sb.Append(customer.C_D_ID);
            sb.Append('-');
            sb.Append(customer.C_LAST);
            var customerKey = sb.ToString();

            if (!customerIndexByLastName.TryGetValue(customerKey, out var customerList))
            {
                customerList = [];
                customerIndexByLastName[customerKey] = customerList;
            }
            customerList.Add(key);
        }
    }






}
