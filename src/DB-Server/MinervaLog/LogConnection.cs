using System.Threading.Tasks;
using System.Collections.Generic;
using Minerva.DB_Server.Network;
using System;
using Microsoft.Extensions.Logging;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using System.Diagnostics;

namespace Minerva.DB_Server.MinervaLog;

public class LogConnections : ILogConnections
{
    private readonly Cluster _cluster;

    private readonly ILogger _logger = LoggerManager.GetLogger();

    public LogConnections(Cluster cluster)
    {
        _cluster = cluster;
    }


    public void AcknowledgeBatch(int sourceReplicaId, int batchId)
    {
        _ = _cluster.Nodes[sourceReplicaId].Sender.SendMessage(new BatchAckMsg { BatchId = batchId, SourceReplicaId = sourceReplicaId, FromId = _cluster.SelfNode.Id });
    }

    public void BroadcastBatch(Batch batch)
    {
        var batchMsg = Utils.ConvertToBatchMsg(batch);
        _ = _cluster.BroadcastMessageWithoutFieldNumber(batchMsg, false, 1);
    }

    public void BroadcastPoA(int sourceReplicaId, int batchId)
    {
        var grpcBatchId = new PoAMsg { BatchId = batchId, SourceReplicaId = sourceReplicaId };

        _ = _cluster.BroadcastMessage(grpcBatchId);
    }


    public void RequestBatches((int sourceReplicaId, int batchId)[] missingBatches)
    {
        if (missingBatches == null || missingBatches.Length == 0)
        {
            return;
        }

        for (int i = 0; i < missingBatches.Length; i++)
        {
            var (sourceReplicaId, batchId) = missingBatches[i];
            RequestSingleBatch(sourceReplicaId, batchId);
        }

        // Wait for all batch requests to complete
    }

    private void RequestSingleBatch(int sourceReplicaId, int batchId)
    {
        // broadcast without blocking but return the first response
        var batchReq = new BatchRequestMsg { SourceReplicaId = sourceReplicaId, BatchId = batchId, FromId = _cluster.SelfNode.Id };

        // Create tasks for all replicas except self
        _ = _cluster.BroadcastMessage(batchReq);
    }


    public void RequestBatch(int sourceReplicaId, int batchId)
    {
        RequestSingleBatch(sourceReplicaId, batchId);
    }

    public void RequestGlobalCommit(int[] ConsistentCutIndices, int epochId)
    {
        // formatted as "1,2,3:4" where 1,2,3 are the indices and 4 is the epochId
        var req = $"{string.Join(",", ConsistentCutIndices)}:{epochId}";
        //_raftNode.MakeRequest(req);
    }

    public void SendCommittedGlobalCommitIdx(int[] ConsistentCutIndices, int epochId)
    {
        var consistentCutMsg = new ConsistentCutMsg
        {
            ConsistentCutIndices = [.. ConsistentCutIndices],
            EpochId = epochId
        };
        _ = _cluster.BroadcastMessage(consistentCutMsg, false, 2);
    }

    public void SendBatch(int targetReplicaId, Batch batch)
    {
        var batchMsg = Utils.ConvertToBatchMsg(batch);

        _ = _cluster.Nodes[targetReplicaId].Sender.SendMessage(batchMsg, 1);
    }

    public void BroadcastAtEpoch(int sourceReplicaId, int epochId)
    {
        var epochMsg = new ReplicaEpochAnnoMsg
        {
            ReplicaId = sourceReplicaId,
            EpochId = epochId
        };
        _ = _cluster.BroadcastMessage(epochMsg);
    }
}