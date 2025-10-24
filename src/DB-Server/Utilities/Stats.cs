using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;

namespace Minerva.DB_Server;

public static class Stats
{
    public static int TotalAppliedTx  = 0;
    public static int TotalLocalExecuted  = 0;
    public static int StaledTxs = 0;
    public static int ConflictedTx = 0;
    public static int NonLocalExecutedTx = 0;

    public static long Notes = 0;
    public static long Notes2 = 0;



    public static string GetStats()
    {
        StringBuilder sb = new();
        sb.AppendLine($"Total Transactions: {TotalAppliedTx}");
        sb.AppendLine($"Total Local Transactions: {TotalLocalExecuted}");
        sb.AppendLine($"Staled Transactions: {StaledTxs}");
        sb.AppendLine($"Conflicted Transactions: {ConflictedTx}");
        sb.AppendLine($"Non Local Executed Transactions: {NonLocalExecutedTx}");
        sb.AppendLine($"Notes1: {Notes}, Notes2: {Notes2}");
        return sb.ToString();
    }

}