using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Microsoft.Extensions.Logging;

namespace Minerva.DB_Server.MinervaLog;


public class LogReceiveRequestHandler
{
    private readonly GlobalLogManager _logManager;

    public LogReceiveRequestHandler(GlobalLogManager logManager)
    {
        _logManager = logManager;
    }

    public void HandleReceivedBatch(BatchMsg batchMsg)
    {
        var batch = Utils.ConvertFromBatchMsg(batchMsg);
        _logManager.ReceivedBatch(batch);
    }

    public void HandleReceivedAcknowledgeBatch(BatchAckMsg ackedBatch)
    {
        _logManager.ReceivedAcknowledgement(ackedBatch.FromId, ackedBatch.SourceReplicaId, ackedBatch.BatchId);
    }

    public void HandleReceivedPoA(PoAMsg poa)
    {
        _logManager.ReceivedPoA(poa.SourceReplicaId, poa.BatchId);
    }

    public void HandleReceivedRequestBatch(BatchRequestMsg request)
    {
        _logManager.ReceivedBatchRequest(request.FromId, request.SourceReplicaId, request.BatchId);
    }

    public void HandleReceivedCommittedIndx(ConsistentCutMsg globalCommit)
    {
        _logManager.RaftNode.RaftLog.AppendLog([.. globalCommit.ConsistentCutIndices], globalCommit.EpochId);
    }

    public void HandleReceivedBroadCastCurEpochId(ReplicaEpochAnnoMsg globalCommit)
    {
        _logManager.SetClusterEpochInfo(globalCommit.ReplicaId, globalCommit.EpochId);
    }
}
