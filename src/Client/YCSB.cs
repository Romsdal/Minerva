
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.Grpc_Client;

public class YCSB : IBenchmarkWorkload
{
    public YCSBConfig Config { get; init; }

    private int _loadTxCount = 0;
    public bool IsLoadComplete { get; private set; } = false;

    public float Progress { get; private set; } = 0;

    private readonly RandomDataUtil _randomDataUtil;


    public YCSB(YCSBConfig config, bool preLoadDB)
    {
        Config = config;

        Distribution distribution;
        if (config.ContentionRatio == 0)
        {
            distribution = Distribution.Uniform;
        }
        else
        {
            distribution = Distribution.Zipf;
        }

        if (preLoadDB && !string.IsNullOrEmpty(config.KeyFile))
        {
            _randomDataUtil = new(config.KeySize, config.RecordCount, config.ValueSize, distribution, config.ContentionRatio, config.KeyFile);
        }
        else if (!preLoadDB && string.IsNullOrEmpty(config.KeyFile))
        {
            _randomDataUtil = new(config.KeySize, config.RecordCount, config.ValueSize, distribution, config.ContentionRatio, null);
        }
        else
        {
            throw new ArgumentException("Key file must be provided if not generating keys.");
        }

        GenerateLoadQueries();
    }

    private List<TxInfo> _loadQueries = [];
    public bool GetNextLoadDataQuery(out TxInfo query)
    {
        lock (_loadQueries)
        {
            if (_loadTxCount >= _loadQueries.Count)
            {
                IsLoadComplete = true;
                query = default;
                return false;
            }

            query = _loadQueries[_loadTxCount];

            _loadTxCount++;
            Progress = (float)_loadTxCount / _loadQueries.Count;
            return true;
        }
    }


    public void GenerateLoadQueries()
    {

        for (int i = 0; i < Config.RecordCount / 100; i++)
        {
            List<KV> KVCmds = [];

            for (int j = 0; j < 100; j++)
            {
                var (shard, key) = _randomDataUtil.Keys[i * 100 + j];
                var value = new string('-', Config.ValueSize);
                KV cmd = new()
                {
                    Type = OpType.Set,
                    Shard = shard,
                    Key = key,
                    Value = value
                };
                KVCmds.Add(cmd);
            }

            _loadQueries.Add(new TxInfo
            {
                Type = TxType.YCSB_LD,
                Query = new()
                {
                    Type = QueryType.Ycsb,

                }
            });
            _loadQueries[i].Query.KVCmds = KVCmds;
        }


    }

    public bool GetNextQuery(out TxInfo query)
    {
        List<KV> reqs = new(Config.TransactionSize);
        var keys = _randomDataUtil.GetRandomKey(Config.TransactionSize);


        foreach (var (shard, key) in keys)
        {
            int randomNumber = _randomDataUtil.GetRandomNumber(0, 100);
            OpType opType = default;
            switch (Config.type)
            {
                case YCSBType.A:
                    if (randomNumber < 50)
                    {
                        opType = OpType.Get; // Read
                    }
                    else
                    {
                        opType = OpType.Set; // Write
                    }
                    break;

                case YCSBType.B:
                    if (randomNumber < 95)
                    {
                        opType = OpType.Get; // Read
                    }
                    else
                    {
                        opType = OpType.Set; // Write
                    }
                    break;
                case YCSBType.C:
                    opType = OpType.Set;
                    break;

            }

            KV cmd = new()
            {
                Type = opType,
                Shard = shard,
                Key = key,
            };

            if (opType == OpType.Set)
            {
                cmd.Value = _randomDataUtil.GetRandomValue(Config.ValueSize);
            }

            reqs.Add(cmd);
        }

        query = new TxInfo
        {
            Query = new()
            {
                Type = QueryType.Ycsb,
                KVCmds = reqs
            },
            Type = TxType.YCSB_TX
        };

        return true;
    }


    public void SaveKeys()
    {
        _randomDataUtil.SaveKeysToFile("ycsb_keys.bin");
    }

}