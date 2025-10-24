using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Network;

public interface IReceivedRequestHandler1
{

    public void HandleReceivedBatch(BatchMsg batch);
    public void HandleReceivedAcknowledgeBatch(BatchAckMsg ack);
    public void HandleReceivedPoA(PoAMsg poa);
    public BatchMsg HandleReceivedRequestBatch(BatchRequestMsg request);
    public void HandleReceivedNoRaftGlobalCommit(ConsistentCutMsg globalCommit);
    public void HandleReceivedBroadCastCurEpochId(ReplicaEpochAnnoMsg globalCommit);
}


public interface IReceivedRequestHandler
{
    public void HandleMessageAsync(Stream stream, object message);
}