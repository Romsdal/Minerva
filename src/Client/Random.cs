using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MemoryPack;
using System.Text.Json;

namespace Minerva.Grpc_Client;



public enum Distribution
{
    Uniform,
    Zipf
}

public class RandomDataUtil
{
    private const int SHARD_SIZE = 100000;
    private const int CACHE_SIZE = 1000;
    private readonly Random _random = new();
    private readonly string[] _valueCache = new string[CACHE_SIZE];
    // shard -> list of keys
    public readonly List<(int, string)> Keys = new();
    private readonly Distribution _distribution;
    private readonly float _skewness;
    private readonly int _keySize;
    private readonly int _valueSize;
    private double[] _zipfProbabilities;

    public RandomDataUtil(int keySize, int recordCount, int valueSize, Distribution dist, float skewness = 0, string storedKeyFile = null)
    {

        if (dist == Distribution.Zipf && skewness <= 0)
        {
            throw new ArgumentException("Skewness must be greater than 0 for Zipf distribution.");
        }

        _distribution = dist;
        _skewness = skewness;
        _keySize = keySize;
        _valueSize = valueSize;

        HashSet<string> uniqueKeys = [];

        if (!string.IsNullOrEmpty(storedKeyFile))
        {
            // load keys from file
            var loadedKeys = MemoryPackSerializer.Deserialize<List<(int, string)>>(File.ReadAllBytes(storedKeyFile));
            if (loadedKeys.Count != recordCount)
            {
                throw new ArgumentException($"Stored key file does not contain the same number of keys need ({recordCount} vs file {loadedKeys.Count})");
            }
            if (loadedKeys.Any(k => k.Item2.Length != keySize))
            {
                throw new ArgumentException($"Stored key file contains keys with different size than needed ({keySize})");
            }
            Keys.AddRange(loadedKeys.Take(recordCount));
        }
        else
        {
            // generate keys
            while (uniqueKeys.Count < recordCount)
            {
                string key = GenerateRandomString(keySize);
                uniqueKeys.Add(key);
            }

            // Assign shard IDs: every 100k keys is a shard, starting with shard id 0

            int shardId = 0;
            int keysInCurrentShard = 0;

            foreach (var key in uniqueKeys)
            {
                Keys.Add((shardId, key));
                keysInCurrentShard++;

                if (keysInCurrentShard >= SHARD_SIZE)
                {
                    shardId++;
                    keysInCurrentShard = 0;
                }
            }
        }




        // pre generate values cache
        for (int i = 0; i < CACHE_SIZE; i++)
        {
            _valueCache[i] = GenerateRandomString(valueSize);
        }

        // Initialize Zipf probabilities if using Zipf distribution
        if (_distribution == Distribution.Zipf)
        {
            InitializeZipfProbabilities();
        }
    }


    public (int, string)[] GetRandomKey(int numKeys)
    {
        (int, string)[] keys = new (int, string)[numKeys];
        if (_distribution == Distribution.Uniform)
        {
            for (int i = 0; i < numKeys; i++)
            {
                keys[i] = Keys[_random.Next(Keys.Count)];
            }
        }
        else // Zipf
        {
            for (int i = 0; i < numKeys; i++)
            {
                int index = GetZipfDistributionIndex();
                keys[i] = Keys[index];
            }
        }

        return keys;
    }

    public string GetRandomValue(int size, bool useCache = true)
    {
        if (useCache && size == _valueSize)
        {
            return _valueCache[_random.Next(_valueCache.Length)];
        }
        else
        {
            return GenerateRandomString(size);
        }
    }

    private string GenerateRandomString(int length)
    {
        const string chars = "abcdefghijklmnorqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string([.. Enumerable.Repeat(chars, length).Select(s => s[_random.Next(s.Length)])]);
    }

    private void InitializeZipfProbabilities()
    {
        // Calculate the normalization constant (sum of 1/i^skewness)
        double norm = 0.0;
        for (int i = 1; i <= Keys.Count; i++)
        {
            norm += 1.0 / Math.Pow(i, _skewness);
        }

        // Calculate cumulative probabilities for efficient sampling
        _zipfProbabilities = new double[Keys.Count];
        double cumulative = 0.0;
        for (int i = 0; i < Keys.Count; i++)
        {
            double probability = (1.0 / Math.Pow(i + 1, _skewness)) / norm;
            cumulative += probability;
            _zipfProbabilities[i] = cumulative;
        }
    }

    private int GetZipfDistributionIndex()
    {
        // Generate a random value between 0 and 1
        double randomValue = _random.NextDouble();

        // Binary search to find the index corresponding to the random value
        int left = 0;
        int right = _zipfProbabilities.Length - 1;

        while (left < right)
        {
            int mid = (left + right) / 2;
            if (_zipfProbabilities[mid] < randomValue)
            {
                left = mid + 1;
            }
            else
            {
                right = mid;
            }
        }

        return left;
    }

    public int GetRandomNumber(int min, int max)
    {
        return _random.Next(min, max);
    }


    public void SaveKeysToFile(string filePath)
    {
        // use memorypack
        var bytes = MemoryPackSerializer.Serialize(Keys);
        File.WriteAllBytes(filePath, bytes);

    }


}

