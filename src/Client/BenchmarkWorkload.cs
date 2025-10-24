

using System;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.Grpc_Client;

public enum TxType
{
    All,
    YCSB_TX,
    YCSB_LD,

    TPCC_NO,
    TPCC_PA,
    TPCC_LD,

}

public class TxInfo
{
    public ClientRequest Query;
    public TxType Type;
    public DateTime StartTime;
    public DateTime EndTime;
    public TxResult Result;
}

public interface IBenchmarkWorkload
{
    
    public bool IsLoadComplete { get; }
    public float Progress { get; }
    public bool GetNextLoadDataQuery(out TxInfo query);
    public bool GetNextQuery(out TxInfo query);

}