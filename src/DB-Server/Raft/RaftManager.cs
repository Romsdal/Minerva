using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using DotNext.IO;
using DotNext.IO.Log;
using DotNext.Net.Cluster.Consensus.Raft;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.Network;
namespace Minerva.DB_Server.Raft;


public class RaftManager
{

    public event Action<RaftClusterMember> OnLeaderChanged;
    public readonly RaftLog RaftLog = new();
    private readonly RaftCluster _cluster;
    private readonly CancellationTokenSource _cts = new();
    public bool IsCurrentNodeLeader { get; private set; } = false;

    private readonly ILogger _logger = LoggerManager.GetLogger();
    

    public RaftManager(Node[] nodes)
    {
        RaftCluster.NodeConfiguration config = null;
        foreach (var node in nodes)
        {
            if (node.IsSelfNode)
            {
                config = new RaftCluster.TcpConfiguration(new IPEndPoint(IPAddress.Parse(node.Address), node.Port + Consts.Network.RAFT_PORT_OFFSET))
                {
                    RequestTimeout = TimeSpan.FromMilliseconds(140),
                    LowerElectionTimeout = 150,
                    UpperElectionTimeout = 300,
                    TransmissionBlockSize = 4096,
                    ColdStart = false,
                };
                break;
            }
        }

        var builder = config.UseInMemoryConfigurationStorage().CreateActiveConfigurationBuilder();
        foreach (var node in nodes)
        {
            builder.Add(new IPEndPoint(IPAddress.Parse(node.Address), node.Port + Consts.Network.RAFT_PORT_OFFSET));
        }
        builder.Build();

        _cluster = new RaftCluster(config);

        _cluster.LeaderChanged += (cluster, leader) =>
        {
            _logger.LogInformation("Raft leader changed to: {Leader}", leader?.EndPoint);

            if (leader.EndPoint.Equals(_cluster.LocalMemberAddress))
            {
                IsCurrentNodeLeader = true;
            }
            else
            {
                IsCurrentNodeLeader = false;
            }
            
            OnLeaderChanged.Invoke(leader);
        };
        _cluster.AuditTrail = new ConsensusOnlyState();

        
    }

    public async Task Init()
    {
        await _cluster.StartAsync(_cts.Token);
        _cluster.Readiness.Wait();

        _cluster.WaitForLeaderAsync(new TimeSpan(0, 0, 10), _cts.Token).Wait();


        IsCurrentNodeLeader = _cluster.Leader.EndPoint.Equals(_cluster.LocalMemberAddress);
        
        _logger.LogInformation("Raft cluster is ready. Current node: {NodeId}, leader: {Leader}", _cluster.LocalMemberAddress, _cluster.Leader.EndPoint);
    }

    // public bool IsLeader()
    // {
    //     return _cluster.Leader.EndPoint.Equals(_cluster.LocalMemberAddress);
    // }

    public async Task LeaderReplicate(List<int> indices, int epochId)
    {
        // convert List<int> to byte[]
        byte[] message = new byte[indices.Count * 4 + 4];
        BitConverter.GetBytes(epochId).CopyTo(message, 0);
        for (int i = 0; i < indices.Count; i++)
        {
            BitConverter.GetBytes(indices[i]).CopyTo(message, i * 4 + 4);
        }

        var res = await _cluster.ReplicateAsync(message, _cts.Token);
        if (!res)
        {
            _logger.LogError("Leader replication failed");
        }
        RaftLog.AppendLog(indices, epochId);
    }


    public void Dispose()
    {
        // handle the stopping cluster when cancellation is requested
        _cluster.StopAsync(CancellationToken.None).Wait();
        _cts.Cancel();
        _cluster.Dispose();
    }

}