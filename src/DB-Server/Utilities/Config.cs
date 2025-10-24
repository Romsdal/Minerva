using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Minerva.DB_Server.Storage;

namespace Minerva.DB_Server;

public class MinervaConfig
{
    [JsonInclude]
    public string ReadStorage { get; init; }
    [JsonInclude]
    public string[] DatabaseToLoad { get; init; }
    [JsonInclude]
    public bool SolverExact { get; init; }
    [JsonInclude]
    public int[] ReplicaPriority { get; init; }
    [JsonInclude]
    public int LocalEpochInterval { get; init; }
    [JsonInclude]
    public int CoordinatorGlobalEpochInterval { get; init; }


    public int MaxBatchSize { get; init; } = 4000000; // default 4MB

    public static MinervaConfig ParseConfigJson(string filename)
    {
        var config = JsonSerializer.Deserialize<MinervaConfig>(File.ReadAllText(filename));

        if (config.CoordinatorGlobalEpochInterval < config.LocalEpochInterval)
        {
            throw new Exception("Config: CoordinatorGlobalEpochInterval must be greater than LocalEpochInterval");
        }

        return config;
    }
}

public class NodeInfo()
{
    [JsonInclude]
    public int Id { get; init; }
    [JsonInclude]
    public string Address { get; init; }
    [JsonInclude]
    public int Port { get; init; }
    [JsonInclude]
    public bool IsSelfNode { get; init; }


    public static NodeInfo[] ParseConfigJson(string filename)
    {
        var nodes = JsonSerializer.Deserialize<List<NodeInfo>>(File.ReadAllText(filename));

        // sanity check
        // check if multiple selves
        int selfNodeCount = 0;
        // check if duplicate nodes
        HashSet<string> addrPortSet = [];
        NodeInfo selfNode = null;

        foreach (var n in nodes)
        {
            if (n.IsSelfNode)
            {
                selfNode = n;
                selfNodeCount++;
            }

            if (selfNodeCount > 1)
            {
                throw new Exception("Config: Too many self node!");
            }

            // check for port validity
            if (n.Port < 0 || n.Port > 62535)
            {
                throw new Exception($"Config: Invalid port number {n.Port}");
            }

            string addrPort = n.Address + n.Port.ToString();
            if (addrPortSet.Contains(addrPort))
                throw new Exception("Duplicate nodes!");
            else
                addrPortSet.Add(addrPort);
        }

        if (selfNodeCount == 0)
        {
            throw new Exception("Config: No self node");
        }

        StringBuilder listingNodes = new("The following nodes are read from config:\n");
        foreach (var n in nodes)
            listingNodes.AppendLine(n.Id + " - " + n.Address + ":" + n.Port.ToString());

        Console.WriteLine(listingNodes.ToString());

        // print self node info
        Console.WriteLine("Self node: " + selfNode.Id + " - " + selfNode.Address + ":" + selfNode.Port.ToString());

        return [.. nodes];
    }


}