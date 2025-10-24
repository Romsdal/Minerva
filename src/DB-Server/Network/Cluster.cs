using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Raft;


namespace Minerva.DB_Server.Network;

public class Node : IDisposable
{
    public int Id { get; init; }
    public int Port { get; init; }
    public string Address { get; init; }
    public bool IsSelfNode { get; init; }

    public ServerSender Sender { get; init; }
    public bool IsConnected { get; set; } = false;
    public bool IsAlive { get; set; } = false;


    ILogger _logger = LoggerManager.GetLogger();

    public Node(int id, string address, int port, bool isSelfNode)
    {
        Id = id;
        Address = address;
        Port = port;
        IsSelfNode = isSelfNode;

        if (isSelfNode)
        {
            IsAlive = true;
        }

        Sender = new ServerSender(address, port);

    }

    public async Task Connect()
    {
        try
        {
            await Sender.ConnectAsync();
            IsConnected = true;
        }
        catch (Exception ex)
        {
            _logger.LogError("Failed to connect to node {Id} at {Address}:{Port} - {ex.Message} {stacktrace}", Id, Address, Port, ex.Message, ex.StackTrace);
        }
    }

    public void SendPing(int myId)
    {
        Sender.SendMessage(new PingRequest()
        {
            NodeId = myId
        }).Wait();
    }

    public void Dispose()
    {
        if (IsConnected)
        {
            Sender.Dispose();
        }
        IsConnected = false;
        GC.SuppressFinalize(this);
    }


}

public class Cluster : IDisposable
{
    public Node SelfNode { get; init; }
    public Node[] Nodes { get; init; }

    private readonly ILogger _logger = LoggerManager.GetLogger();


    public Cluster(Node selfNode, Node[] nodes)
    {
        SelfNode = selfNode;
        Nodes = nodes;

    }

    const int numRetries = 60;
    public void Initialize()
    {

        for (int i = 0; i < numRetries; i++)
        {
            var nodesToConnect =
                from node in Nodes
                where node.IsConnected == false
                select node.Connect();


            Task.WaitAll([.. nodesToConnect], new TimeSpan(0, 5, 0));

            if (IsInitialized())
            {
                break;
            }

            _logger.LogWarning($"Cluster initialization failed, retrying");

            foreach (var node in Nodes)
            {
                if (!node.IsConnected)
                {
                    _logger.LogWarning("Node {node.Id} at {node.Address}:{node.Port} is not connected", node.Id, node.Address, node.Port);
                }
            }

            Thread.Sleep(5000); // wait before retrying
        }

        if (!IsInitialized())
        {
            throw new InvalidOperationException("Cluster initialization failed - not all nodes connected");
        }

        foreach (var node in Nodes)
        {
            if (!node.IsSelfNode)
            {
                node.SendPing(SelfNode.Id);
            }
        }

        for (int i = 0; i < 5; i++)
        {
            Thread.Sleep(1000); // wait for pings to be processed
            if (AliveNodeIds().Length == Nodes.Length)
            {
                _logger.LogInformation("Cluster initialization succeeded - all nodes alive");
                return;
            }
        }

        throw new InvalidOperationException("Cluster initialization failed - not all nodes alive after ping");


    }

    public bool IsInitialized()
    {
        foreach (var node in Nodes)
        {
            if (!node.IsConnected)
                return false;
        }

        return true;
    }

    public int[] AliveNodeIds()
    {
        return [.. from node in Nodes
                where node.IsAlive
                select node.Id];
    }


    public async Task BroadcastMessage<T>(T message, bool includeSelf = false, int streamToUse = 0)
    {
        List<Task> sendTasks = [];
        using var memoryOwner = ProtoMessageHelper.SerializeToMemoryOwnerWithFieldNumber(message);

        foreach (var node in Nodes)
        {
            if (includeSelf || !node.IsSelfNode)
            {
                sendTasks.Add(node.Sender.SendMessageBytes(memoryOwner.Memory, streamToUse));
            }
        }
        await Task.WhenAll(sendTasks);


    }

    public async Task BroadcastMessageWithoutFieldNumber<T>(T message, bool includeSelf = false, int streamToUse = 0)
    {
        List<Task> sendTasks = [];
        using var memoryOwner = ProtoMessageHelper.SerializeToMemoryOwner(message);

        foreach (var node in Nodes)
        {
            if (includeSelf || !node.IsSelfNode)
            {
                sendTasks.Add(node.Sender.SendMessageBytes(memoryOwner.Memory, streamToUse));
            }
        }
        await Task.WhenAll(sendTasks);

    }


    public void Dispose()
    {
        foreach (var node in Nodes)
        {
            node.Dispose();
        }
        GC.SuppressFinalize(this);
    }

    public static Cluster CreateFromConfig(NodeInfo[] nodesInfo, CancellationToken ct)
    {
        Node[] nodes = new Node[nodesInfo.Length];
        Node selfNode = null;
        foreach (var n in nodesInfo)
        {
            nodes[n.Id] = new Node(n.Id, n.Address, n.Port, n.IsSelfNode);
            selfNode = n.IsSelfNode ? nodes[n.Id] : selfNode;
        }

        return new Cluster(selfNode, nodes);
    }
}