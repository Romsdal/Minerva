using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server;
using Minerva.DB_Server.Interface;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using ProtoBuf;

public class ServerReceivedRequestHandler : IReceivedRequestHandler
{
    private Cluster _cluster;
    private LogReceiveRequestHandler _logReqHandler;

    private readonly ILogger _logger = LoggerManager.GetLogger();


    public ServerReceivedRequestHandler(Cluster cluster, LogReceiveRequestHandler logReqHandler)
    {
        _cluster = cluster;
        _logReqHandler = logReqHandler;
    }



    public void HandleMessageAsync(Stream stream, object message)
    {
        ArgumentNullException.ThrowIfNull(message);


        switch (message)
        {
            case PingRequest pingReq:
                var reply = new PingReply { NodeId = _cluster.SelfNode.Id };
                ////_logger.LogTrace("Received PingRequest from Node {Id}, sending PingReply", ((PingRequest)message).NodeId);
                _cluster.Nodes[pingReq.NodeId].IsAlive = true;

                while (!_cluster.Nodes[pingReq.NodeId].IsConnected)
                {
                    ////_logger.LogTrace("Waiting for connection to Node {Id} to be established...", pingReq.NodeId);
                    Thread.Sleep(100);
                }

                _ = _cluster.Nodes[pingReq.NodeId].Sender.SendMessage(reply);
                break;
            case PingReply pingRep:
                int id = pingRep.NodeId;
                ////_logger.LogTrace("Received PingReply from Node {Id}", id);
                _cluster.Nodes[pingRep.NodeId].IsAlive = true;
                break;

            case BatchAckMsg batchAck:
                _logReqHandler.HandleReceivedAcknowledgeBatch(batchAck);
                break;
            case PoAMsg poaMsg:
                _logReqHandler.HandleReceivedPoA(poaMsg);
                break;
            case BatchRequestMsg batchRequest:
                _logReqHandler.HandleReceivedRequestBatch(batchRequest);
                break;
            case ConsistentCutMsg consistentCutMsg:
                _logReqHandler.HandleReceivedCommittedIndx(consistentCutMsg);
                break;
            case ReplicaEpochAnnoMsg epochAnnoMsg:
                _logReqHandler.HandleReceivedBroadCastCurEpochId(epochAnnoMsg);
                break;
            default:
                _logger.LogError("Unknown message type received: {MessageType}", message.GetType().Name);
                break;
        }
    }
}



public class ReceivedClientRequestHandler : IReceivedRequestHandler
{

    private ProtoClientInterface _protoClientInterface;

    private readonly ILogger _logger = LoggerManager.GetLogger();

    public ReceivedClientRequestHandler(ProtoClientInterface protoClientInterface)
    {
        _protoClientInterface = protoClientInterface;
    }


    public async void HandleMessageAsync(Stream stream, object message)
    {
        TxResult result;

        ClientRequest msg = message as ClientRequest;

        try
        {
            result = await _protoClientInterface.NewQuery(msg);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process client request {q}", msg);
            TxResult txResult = new()
            {
                SeqId = msg.SeqId,
                Executed = false,
                TxResultStr = $"Error processing request: {ex.Message}"
            };

            result = txResult;
        }

        if (result != null)
        {
            try
            {

                //Serializer.SerializeWithLengthPrefix(stream, result, PrefixStyle.Fixed32BigEndian);
                using var memoryOwner = ProtoMessageHelper.SerializeToMemoryOwner(result);
                await stream.WriteAsync(memoryOwner.Memory);

            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
        }
    }
}


public class ServerReceivedBatchHandler : IReceivedRequestHandler
{
    private LogReceiveRequestHandler _logReqHandler;

    private readonly ILogger _logger = LoggerManager.GetLogger();


    public ServerReceivedBatchHandler(LogReceiveRequestHandler logReqHandler)
    {
        _logReqHandler = logReqHandler;
    }

    public void HandleMessageAsync(Stream stream, object message)
    {

        BatchMsg msg = message as BatchMsg;
        _logReqHandler.HandleReceivedBatch(msg);
        
    }
}