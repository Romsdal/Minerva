using System.Collections.Generic;
using System.Threading;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server;

public partial class Utils
{
    public static BatchMsg ConvertToBatchMsg(Batch batch)
    {
        var batchMsg = new BatchMsg
        {
            Status = (GrpcBatchStatus)(int)batch.Status,
            BatchId = batch.BatchId,
            SourceReplicaId = batch.SourceReplicaId,
            Transactions = []
        };
    

        batchMsg.Transactions = batch.Transactions;

        return batchMsg;
    }

    public static Batch ConvertFromBatchMsg(BatchMsg batchMsg)
    {
        var batch = new Batch(batchMsg.BatchId, batchMsg.SourceReplicaId)
        {
            Status = (BatchStatus)(int)batchMsg.Status,
            Transactions = batchMsg.Transactions ?? []
        };

        return batch;
    }
}