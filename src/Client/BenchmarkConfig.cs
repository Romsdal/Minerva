using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Minerva.Grpc_Client;


public enum BenchmarkType
{
    YCSB,
    TPCC
}

public class BenchmarkConfig
{
    public string BenchmarkName;
    // clients per server
    public int Clients;
    public bool PreLoadDB;
    // duration of the test run in seconds
    public uint Duration = 60;
    public string[] Servers;


    public BenchmarkType Type;
    public YCSBConfig YCSBConfig;
    public TPCCConfig TPCCConfig;

    public BenchmarkConfig()
    {
    }


    public static BenchmarkConfig ParseConfigFromJson(string configPath)
    {
        try
        {
            string jsonString = File.ReadAllText(configPath);
            
            using JsonDocument document = JsonDocument.Parse(jsonString);
            JsonElement root = document.RootElement;
            
            var config = new BenchmarkConfig();

            // Parse main benchmark config
            if (root.TryGetProperty("BenchmarkConfig", out JsonElement benchmarkElement))
            {
                if (benchmarkElement.TryGetProperty("benchmark_name", out JsonElement nameElement))
                    config.BenchmarkName = nameElement.GetString();

                if (benchmarkElement.TryGetProperty("clients", out JsonElement clientsElement))
                    config.Clients = clientsElement.GetInt32();

                if (benchmarkElement.TryGetProperty("pre_load_db", out JsonElement preLoadElement))
                    config.PreLoadDB = preLoadElement.GetBoolean();

                if (benchmarkElement.TryGetProperty("duration", out JsonElement durationElement))
                    config.Duration = durationElement.GetUInt32();

                if (benchmarkElement.TryGetProperty("benchmark_type", out JsonElement typeElement))
                {
                    string typeStr = typeElement.GetString();
                    config.Type = Enum.Parse<BenchmarkType>(typeStr, true);
                }
                
                if (benchmarkElement.TryGetProperty("servers", out JsonElement serversElement) && serversElement.ValueKind == JsonValueKind.Array)
                {
                    config.Servers = new string[serversElement.GetArrayLength()];
                    int index = 0;
                    foreach (var server in serversElement.EnumerateArray())
                    {
                        config.Servers[index++] = server.GetString();
                    }
                }
            }
            
            // Parse YCSB config if present
            if (root.TryGetProperty("YCSBConfig", out JsonElement ycsbElement))
            {
                config.YCSBConfig = ParseYCSBConfigFromJson(ycsbElement);
            }
            
            // Parse TPCC config if present
            if (root.TryGetProperty("TPCCConfig", out JsonElement tpccElement))
            {
                config.TPCCConfig = ParseTPCCConfigFromJson(tpccElement);
            }
            
            return config;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to parse benchmark configuration from {configPath}: {ex.Message}", ex);
        }
    }


    private static YCSBConfig ParseYCSBConfigFromJson(JsonElement ycsbElement)
    {
        var config = new YCSBConfig();
        
        if (ycsbElement.TryGetProperty("YCSB_type", out JsonElement typeElement))
        {
            string typeStr = typeElement.GetString();
            config.type = Enum.Parse<YCSBType>(typeStr, true);
        }
        
        if (ycsbElement.TryGetProperty("contention_ratio", out JsonElement contentionElement))
            config.ContentionRatio = contentionElement.GetSingle();
        
        if (ycsbElement.TryGetProperty("transaction_size", out JsonElement transactionSizeElement))
            config.TransactionSize = transactionSizeElement.GetInt32();
        
        if (ycsbElement.TryGetProperty("key_size", out JsonElement keySizeElement))
            config.KeySize = keySizeElement.GetInt32();
        
        if (ycsbElement.TryGetProperty("value_size", out JsonElement valueSizeElement))
            config.ValueSize = valueSizeElement.GetInt32();

        if (ycsbElement.TryGetProperty("record_count", out JsonElement recordCountElement))
            config.RecordCount = recordCountElement.GetInt32();

        if (ycsbElement.TryGetProperty("keyfile", out JsonElement keyfileElement))
            config.KeyFile = keyfileElement.GetString();
        
        return config;
    }

    private static TPCCConfig ParseTPCCConfigFromJson(JsonElement tpccElement)
    {
        var config = new TPCCConfig();

        if (tpccElement.TryGetProperty("NumWarehouse", out JsonElement numWarehouseElement))
            config.NumWarehouse = numWarehouseElement.GetInt32();

        if (tpccElement.TryGetProperty("warmupInSeconds", out JsonElement warmupElement))
            config.warmupInSeconds = warmupElement.GetInt32();


        return config;
    }


    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("=== Benchmark Configuration ===");
        sb.AppendLine($"Benchmark Name: {BenchmarkName}");
        sb.AppendLine($"Type: {Type}");
        sb.AppendLine($"Clients per Server: {Clients}");
        sb.AppendLine($"Pre-load DB: {PreLoadDB}");
        sb.AppendLine($"Duration: {Duration}s");
        
        if (Servers != null && Servers.Length > 0)
        {
            sb.AppendLine($"Servers ({Servers.Length}):");
        }
        
        if (Type == BenchmarkType.YCSB && YCSBConfig.RecordCount > 0)
        {
            sb.AppendLine("\n=== YCSB Configuration ===");
            sb.AppendLine($"YCSB Type: {YCSBConfig.type}");
            sb.AppendLine($"Contention Ratio: {YCSBConfig.ContentionRatio}");
            sb.AppendLine($"Transaction Size: {YCSBConfig.TransactionSize}");
            sb.AppendLine($"Key Size: {YCSBConfig.KeySize} bytes");
            sb.AppendLine($"Value Size: {YCSBConfig.ValueSize} bytes");
            sb.AppendLine($"Record Count: {YCSBConfig.RecordCount}");
        }
        
        if (Type == BenchmarkType.TPCC && TPCCConfig.NumWarehouse > 0)
        {
            sb.AppendLine("\n=== TPC-C Configuration ===");
            sb.AppendLine($"Number of Warehouses: {TPCCConfig.NumWarehouse}");
            sb.AppendLine($"Warmup Duration: {TPCCConfig.warmupInSeconds}s");
        }
        
        return sb.ToString();
    }

}

public enum YCSBType
{
    A,
    B,
    C
}


public struct YCSBConfig
{
    public YCSBType type;
    public float ContentionRatio;
    public int TransactionSize = 10;
    // 10 bytes 
    public int KeySize = 10;
    // 1000 bytes
    public int ValueSize = 1000;
    // Total of 1GB datasize
    public int RecordCount = 1000000;
    public string KeyFile = "";

    public YCSBConfig()
    {
    }
}


public struct TPCCConfig
{
    public int NumWarehouse;
    public int warmupInSeconds = 10;

    public TPCCConfig()
    {
        
    }

}