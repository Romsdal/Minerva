using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Minerva.DB_Server.MinervaLog;

public interface ILogConnections
{
    void BroadcastBatch(Batch batch);
    void AcknowledgeBatch(int sourceReplicaId, int batchId);
    void BroadcastPoA(int sourceReplicaId, int batchId);
    void RequestBatches((int sourceReplicaId, int batchId)[] missingBatches);
    void RequestBatch(int sourceReplicaId, int batchId);
    void SendBatch(int targetReplicaId, Batch batch);

    void RequestGlobalCommit(int[] ConsistentCutIndices, int epochId);
    void SendCommittedGlobalCommitIdx(int[] ConsistentCutIndices, int epochId);

    void BroadcastAtEpoch(int sourceReplicaId, int epochId);

}