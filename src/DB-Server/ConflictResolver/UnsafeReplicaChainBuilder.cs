using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.ConflictResolver;

public sealed unsafe class UnsafeReplicaChainBuilder : IDisposable
{
    private const float MapLoadFactor = 0.72f;
    private const int MinimumCapacity = 64;

    private TransactionRecord[] _recordBuffer = Array.Empty<TransactionRecord>();
    private RootIndexPair[] _pairBuffer = Array.Empty<RootIndexPair>();

    private int* _parent;
    private byte* _rank;
    private int _bufferCapacity;

    private UnsafeIntMap _indexMap;

    private int _count;

    public UnsafeReplicaChainBuilder()
    {
        _indexMap = new UnsafeIntMap(MapLoadFactor, MinimumCapacity);
    }

    public void BuildChains(int replicaId, List<Batch> batches, List<TransactionsChain> destination)
    {
        if (batches == null || batches.Count == 0)
        {
            return;
        }

        CollectTransactions(replicaId, batches);

        if (_count == 0)
        {
            return;
        }

        BuildComponents();
        EmitChains(replicaId, destination);
    }

    private void CollectTransactions(int replicaId, List<Batch> batches)
    {
        int required = 0;
        foreach (var batch in batches)
        {
            if (batch?.Transactions == null)
            {
                continue;
            }

            foreach (var tx in batch.Transactions)
            {
                if (tx is null || tx.ConflictStatus == TxConflict.NonExecuted)
                {
                    continue;
                }

                required++;
            }
        }

        EnsureCapacity(required);
        _indexMap.Reset(required);

        _count = 0;

        foreach (var batch in batches)
        {
            if (batch?.Transactions == null)
            {
                continue;
            }

            foreach (var tx in batch.Transactions)
            {
                if (tx is null || tx.ConflictStatus == TxConflict.NonExecuted)
                {
                    continue;
                }

                tx.SourceReplicaId = replicaId;

                _recordBuffer[_count] = tx;
                _parent[_count] = _count;
                _rank[_count] = 0;

                _indexMap.AddOrSet(tx.Tid, _count);
                _count++;
            }
        }
    }

    private void BuildComponents()
    {
        for (int i = 0; i < _count; i++)
        {
            var tx = _recordBuffer[i];
            if (tx?.PrevTids == null || tx.PrevTids.Count == 0)
            {
                continue;
            }

            foreach (int prev in tx.PrevTids)
            {
                if (_indexMap.TryGetValue(prev, out int idx))
                {
                    Union(i, idx);
                }
            }
        }

        for (int i = 0; i < _count; i++)
        {
            _pairBuffer[i].Root = Find(i);
            _pairBuffer[i].Index = i;
        }

        Array.Sort(_pairBuffer, 0, _count, RootComparer.Instance);
    }

    private void EmitChains(int replicaId, List<TransactionsChain> destination)
    {
        int index = 0;
        while (index < _count)
        {
            int root = _pairBuffer[index].Root;
            int start = index;

            while (index < _count && _pairBuffer[index].Root == root)
            {
                index++;
            }

            int componentSize = index - start;
            var chain = new TransactionsChain(replicaId);
            chain.Records.EnsureCapacity(componentSize);

            for (int i = start; i < index; i++)
            {
                var tx = _recordBuffer[_pairBuffer[i].Index];
                chain.Records.Add(tx);
            }

            destination.Add(chain);
        }
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _bufferCapacity)
        {
            return;
        }

        int newCapacity = NextPowerOfTwo(Math.Max(required, MinimumCapacity));

        Array.Resize(ref _recordBuffer, newCapacity);
        Array.Resize(ref _pairBuffer, newCapacity);

        if (_parent == null)
        {
            _parent = (int*)NativeMemory.Alloc((nuint)newCapacity, (nuint)sizeof(int));
        }
        else
        {
            _parent = (int*)NativeMemory.Realloc(_parent, (nuint)(newCapacity * sizeof(int)));
        }

        if (_rank == null)
        {
            _rank = (byte*)NativeMemory.AllocZeroed((nuint)newCapacity, 1);
        }
        else
        {
            byte* newRank = (byte*)NativeMemory.Realloc(_rank, (nuint)newCapacity);
            if (newRank == null)
            {
                throw new OutOfMemoryException();
            }

            Unsafe.InitBlockUnaligned(newRank + _bufferCapacity, 0, (uint)(newCapacity - _bufferCapacity));
            _rank = newRank;
        }

        _bufferCapacity = newCapacity;
    }

    private int Find(int x)
    {
        while (_parent[x] != x)
        {
            _parent[x] = _parent[_parent[x]];
            x = _parent[x];
        }

        return x;
    }

    private void Union(int x, int y)
    {
        int rootX = Find(x);
        int rootY = Find(y);

        if (rootX == rootY)
        {
            return;
        }

        byte rankX = _rank[rootX];
        byte rankY = _rank[rootY];

        if (rankX < rankY)
        {
            _parent[rootX] = rootY;
        }
        else if (rankX > rankY)
        {
            _parent[rootY] = rootX;
        }
        else
        {
            _parent[rootY] = rootX;
            _rank[rootX]++;
        }
    }

    public void Dispose()
    {
        if (_parent != null)
        {
            NativeMemory.Free(_parent);
            _parent = null;
        }

        if (_rank != null)
        {
            NativeMemory.Free(_rank);
            _rank = null;
        }

        _indexMap.Dispose();
        _recordBuffer = Array.Empty<TransactionRecord>();
        _pairBuffer = Array.Empty<RootIndexPair>();
        _bufferCapacity = 0;
        _count = 0;
    }

    private static int NextPowerOfTwo(int value)
    {
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }

    private struct RootIndexPair
    {
        public int Root;
        public int Index;
    }

    private sealed class RootComparer : IComparer<RootIndexPair>
    {
        public static readonly RootComparer Instance = new();

        public int Compare(RootIndexPair x, RootIndexPair y)
        {
            return x.Root.CompareTo(y.Root);
        }
    }
}

internal sealed unsafe class UnsafeIntMap : IDisposable
{
    private readonly float _loadFactor;
    private int _capacity;
    private int _threshold;
    private int _count;

    private int* _keys;
    private int* _values;
    private byte* _states;

    public UnsafeIntMap(float loadFactor, int initialCapacity)
    {
        _loadFactor = loadFactor;
        EnsureCapacityInternal(Math.Max(initialCapacity, 1));
    }

    public void Reset(int required)
    {
        if (required <= _capacity)
        {
            Clear();
            return;
        }

        EnsureCapacityInternal(required);
    }

    public void AddOrSet(int key, int value)
    {
        if (_count >= _threshold)
        {
            Grow();
        }

        Insert(key, value, allowUpdate: true);
    }

    public bool TryGetValue(int key, out int value)
    {
        int mask = _capacity - 1;
        int idx = (int)(Hash((uint)key) & (uint)mask);
        int probed = 0;

        while (probed < _capacity)
        {
            if (_states[idx] == 0)
            {
                break;
            }

            if (_keys[idx] == key)
            {
                value = _values[idx];
                return true;
            }

            idx = (idx + 1) & mask;
            probed++;
        }

        value = default;
        return false;
    }

    public void Clear()
    {
        if (_states != null)
        {
            Unsafe.InitBlockUnaligned(_states, 0, (uint)_capacity);
        }

        _count = 0;
    }

    public void Dispose()
    {
        if (_keys != null)
        {
            NativeMemory.Free(_keys);
            _keys = null;
        }

        if (_values != null)
        {
            NativeMemory.Free(_values);
            _values = null;
        }

        if (_states != null)
        {
            NativeMemory.Free(_states);
            _states = null;
        }

        _capacity = 0;
        _threshold = 0;
        _count = 0;
    }

    private void Insert(int key, int value, bool allowUpdate)
    {
        int mask = _capacity - 1;
        int idx = (int)(Hash((uint)key) & (uint)mask);

        while (true)
        {
            if (_states[idx] == 0)
            {
                _keys[idx] = key;
                _values[idx] = value;
                _states[idx] = 1;
                _count++;
                return;
            }

            if (allowUpdate && _keys[idx] == key)
            {
                _values[idx] = value;
                return;
            }

            idx = (idx + 1) & mask;
        }
    }

    private void Grow()
    {
        int newCapacity = _capacity << 1;
        Rehash(newCapacity);
    }

    private void EnsureCapacityInternal(int required)
    {
        int newCapacity = NextPowerOfTwo(Math.Max(required, MinimumCapacity));
        if (newCapacity <= _capacity)
        {
            Clear();
            return;
        }

        Rehash(newCapacity);
    }

    private void Rehash(int newCapacity)
    {
        int* oldKeys = _keys;
        int* oldValues = _values;
        byte* oldStates = _states;
        int oldCapacity = _capacity;

        _keys = (int*)NativeMemory.AllocZeroed((nuint)newCapacity, (nuint)sizeof(int));
        _values = (int*)NativeMemory.Alloc((nuint)newCapacity, (nuint)sizeof(int));
        _states = (byte*)NativeMemory.AllocZeroed((nuint)newCapacity, 1);
        _capacity = newCapacity;
        _threshold = (int)(newCapacity * _loadFactor);
        _count = 0;

        if (oldKeys != null)
        {
            for (int i = 0; i < oldCapacity; i++)
            {
                if (oldStates[i] != 0)
                {
                    Insert(oldKeys[i], oldValues[i], allowUpdate: false);
                }
            }

            NativeMemory.Free(oldKeys);
            NativeMemory.Free(oldValues);
            NativeMemory.Free(oldStates);
        }
    }

    private static uint Hash(uint value)
    {
        value ^= value >> 16;
        value *= 0x7feb352dU;
        value ^= value >> 15;
        value *= 0x846ca68bU;
        value ^= value >> 16;
        return value;
    }

    private static int NextPowerOfTwo(int value)
    {
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }

    private const int MinimumCapacity = 64;
}
