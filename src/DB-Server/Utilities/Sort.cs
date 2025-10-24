using System.Collections.Generic;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server;

public static class Sort
{
    // Insertion Sort
    public static void SortTransactionsByTID(List<TransactionRecord>[] transactionsPerReplica)
    {
        if (transactionsPerReplica == null)
        {
            return;
        }

        foreach (var transactions in transactionsPerReplica)
        {
            if (transactions == null || transactions.Count <= 1)
            {
                continue;
            }

            for (var i = 1; i < transactions.Count; i++)
            {
                var currentRecord = transactions[i];

                if (currentRecord == null)
                {
                    continue;
                }

                var currentTid = currentRecord.Tid;
                var j = i - 1;

                while (j >= 0)
                {
                    var previousRecord = transactions[j];

                    if (previousRecord == null || previousRecord.Tid <= currentTid)
                    {
                        break;
                    }

                    transactions[j + 1] = previousRecord;
                    j--;
                }

                transactions[j + 1] = currentRecord;
            }
        }
    }
}