using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Minerva.DB_Server.Network;

namespace Minerva.DB_Server.ConflictResolver;

public sealed class ConflictGraphTrackers : IDisposable
{
	private const int DefaultEstimate = 512;

	public UnsafeConflictTracker YCSBWriteTracker { get; }
	public UnsafeConflictTracker WarehouseWriteTracker { get; }
	public UnsafeConflictTracker DistrictWriteTracker { get; }
	public UnsafeConflictTracker CustomerWriteTracker { get; }
	public UnsafeConflictTracker ItemWriteTracker { get; }
	public UnsafeConflictTracker StockWriteTracker { get; }
	public UnsafeConflictTracker HistoryWriteTracker { get; }
	public UnsafeConflictTracker NewOrderWriteTracker { get; }
	public UnsafeConflictTracker OrderWriteTracker { get; }
	public UnsafeConflictTracker OrderLineWriteTracker { get; }

	public UnsafeConflictTracker YCSBReadTrackers { get; }
	public UnsafeConflictTracker WarehouseReadTrackers { get; }
	public UnsafeConflictTracker DistrictReadTrackers { get; }
	public UnsafeConflictTracker CustomerReadTrackers { get; }
	public UnsafeConflictTracker ItemReadTrackers { get; }
	public UnsafeConflictTracker StockReadTrackers { get; }
	public UnsafeConflictTracker HistoryReadTrackers { get; }
	public UnsafeConflictTracker NewOrderReadTrackers { get; }
	public UnsafeConflictTracker OrderReadTrackers { get; }
	public UnsafeConflictTracker OrderLineReadTrackers { get; }

	public ConflictGraphTrackers(int estimatedChainCount)
	{
		int estimatedBuckets = Math.Max(DefaultEstimate, NextPow2(estimatedChainCount * 8));

		YCSBWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		WarehouseWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		DistrictWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		CustomerWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		ItemWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		StockWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		HistoryWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		NewOrderWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		OrderWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		OrderLineWriteTracker = new UnsafeConflictTracker(estimatedBuckets);

		YCSBReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		WarehouseReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		DistrictReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		CustomerReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		ItemReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		StockReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		HistoryReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		NewOrderReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		OrderReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		OrderLineReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
	}

	public void AddRWSet(TransactionsChain txc)
	{
		foreach (var tx in txc.Records)
		{
			var writeSet = tx.WriteSet;
			var readSet = tx.ReadSet;
			int rid = txc.SourceReplicaId;
			int txcIdx = txc.SolverIndex;

			foreach (var (shard, key) in writeSet.YCSBWriteSet.Keys)
			{
				YCSBWriteTracker.Add(KeyHasher.Ycsb(shard, key), rid, txcIdx);
			}

			foreach (var (shard, key) in readSet.YCSBReadKeys)
			{
				YCSBReadTrackers.Add(KeyHasher.Ycsb(shard, key), rid, txcIdx);
			}

			foreach (var key in writeSet.WarehouseWriteSet.Keys)
			{
				WarehouseWriteTracker.Add(KeyHasher.Warehouse(key), rid, txcIdx);
			}

			foreach (var key in readSet.WarehouseReadKeys)
			{
				WarehouseReadTrackers.Add(KeyHasher.Warehouse(key), rid, txcIdx);
			}

			foreach (var (dwid, did) in writeSet.DistrictWriteSet.Keys)
			{
				DistrictWriteTracker.Add(KeyHasher.District(dwid, did), rid, txcIdx);
			}

			foreach (var (dwid, did) in readSet.DistrictReadKeys)
			{
				DistrictReadTrackers.Add(KeyHasher.District(dwid, did), rid, txcIdx);
			}

			foreach (var (cwid, cdid, cid) in writeSet.CustomerWriteSet.Keys)
			{
				CustomerWriteTracker.Add(KeyHasher.Customer(cwid, cdid, cid), rid, txcIdx);
			}

			foreach (var (cwid, cdid, cid) in readSet.CustomerReadKeys)
			{
				CustomerReadTrackers.Add(KeyHasher.Customer(cwid, cdid, cid), rid, txcIdx);
			}

			foreach (var key in writeSet.ItemWriteSet.Keys)
			{
				ItemWriteTracker.Add(KeyHasher.Item(key), rid, txcIdx);
			}

			foreach (var key in readSet.ItemReadKeys)
			{
				ItemReadTrackers.Add(KeyHasher.Item(key), rid, txcIdx);
			}

			foreach (var (swid, siid) in writeSet.StockWriteSet.Keys)
			{
				StockWriteTracker.Add(KeyHasher.Stock(swid, siid), rid, txcIdx);
			}

			foreach (var (swid, siid) in readSet.StockReadKeys)
			{
				StockReadTrackers.Add(KeyHasher.Stock(swid, siid), rid, txcIdx);
			}

			foreach (var (hcid, hdate) in writeSet.HistoryWriteSet.Keys)
			{
				HistoryWriteTracker.Add(KeyHasher.History(hcid, hdate), rid, txcIdx);
			}

			foreach (var (hcid, hdate) in readSet.HistoryReadKeys)
			{
				HistoryReadTrackers.Add(KeyHasher.History(hcid, hdate), rid, txcIdx);
			}

			foreach (var (nowid, nodid, nooid) in writeSet.NewOrderWriteSet.Keys)
			{
				NewOrderWriteTracker.Add(KeyHasher.NewOrder(nowid, nodid, nooid), rid, txcIdx);
			}

			foreach (var (nowid, nodid, nooid) in readSet.NewOrderReadKeys)
			{
				NewOrderReadTrackers.Add(KeyHasher.NewOrder(nowid, nodid, nooid), rid, txcIdx);
			}

			foreach (var (owid, odid, oid) in writeSet.OrderWriteSet.Keys)
			{
				OrderWriteTracker.Add(KeyHasher.Order(owid, odid, oid), rid, txcIdx);
			}

			foreach (var (owid, odid, oid) in readSet.OrderReadKeys)
			{
				OrderReadTrackers.Add(KeyHasher.Order(owid, odid, oid), rid, txcIdx);
			}

			foreach (var (olwid, oldid, oloid, olnumber) in writeSet.OrderLineWriteSet.Keys)
			{
				OrderLineWriteTracker.Add(KeyHasher.OrderLine(olwid, oldid, oloid, olnumber), rid, txcIdx);
			}

			foreach (var (olwid, oldid, oloid, olnumber) in readSet.OrderLineReadKeys)
			{
				OrderLineReadTrackers.Add(KeyHasher.OrderLine(olwid, oldid, oloid, olnumber), rid, txcIdx);
			}
		}
	}

	public void Dispose()
	{
		YCSBWriteTracker.Dispose();
		WarehouseWriteTracker.Dispose();
		DistrictWriteTracker.Dispose();
		CustomerWriteTracker.Dispose();
		ItemWriteTracker.Dispose();
		StockWriteTracker.Dispose();
		HistoryWriteTracker.Dispose();
		NewOrderWriteTracker.Dispose();
		OrderWriteTracker.Dispose();
		OrderLineWriteTracker.Dispose();

		YCSBReadTrackers.Dispose();
		WarehouseReadTrackers.Dispose();
		DistrictReadTrackers.Dispose();
		CustomerReadTrackers.Dispose();
		ItemReadTrackers.Dispose();
		StockReadTrackers.Dispose();
		HistoryReadTrackers.Dispose();
		NewOrderReadTrackers.Dispose();
		OrderReadTrackers.Dispose();
		OrderLineReadTrackers.Dispose();
	}

	private static int NextPow2(int value)
	{
		value--;
		value |= value >> 1;
		value |= value >> 2;
		value |= value >> 4;
		value |= value >> 8;
		value |= value >> 16;
		return value + 1;
	}
}

public static class KeyHasher
{
	private const ulong FnvOffset = 14695981039346656037UL;
	private const ulong FnvPrime = 1099511628211UL;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ulong Begin(KeySpace space) => Mix(FnvOffset, (ulong)space);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ulong Mix(ulong hash, ulong value)
	{
		hash ^= value;
		hash *= FnvPrime;
		return hash;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ulong Finalize(ulong hash)
	{
		hash ^= hash >> 33;
		hash *= 0xff51afd7ed558ccdUL;
		hash ^= hash >> 33;
		hash *= 0xc4ceb9fe1a85ec53UL;
		hash ^= hash >> 33;
		return hash;
	}

	public static ulong Ycsb(int shard, string key)
	{
		ulong hash = Begin(KeySpace.Ycsb);
		hash = Mix(hash, (ulong)(uint)shard);

		if (!string.IsNullOrEmpty(key))
		{
			foreach (char c in key)
			{
				hash = Mix(hash, (ulong)(ushort)c);
			}
		}

		return Finalize(hash);
	}

	public static ulong Warehouse(long warehouseId)
	{
		ulong hash = Begin(KeySpace.Warehouse);
		hash = Mix(hash, (ulong)warehouseId);
		return Finalize(hash);
	}

	public static ulong District(long dwid, long did)
	{
		ulong hash = Begin(KeySpace.District);
		hash = Mix(hash, (ulong)dwid);
		hash = Mix(hash, (ulong)did);
		return Finalize(hash);
	}

	public static ulong Customer(long cwid, long cdid, long cid)
	{
		ulong hash = Begin(KeySpace.Customer);
		hash = Mix(hash, (ulong)cwid);
		hash = Mix(hash, (ulong)cdid);
		hash = Mix(hash, (ulong)cid);
		return Finalize(hash);
	}

	public static ulong Item(long itemId)
	{
		ulong hash = Begin(KeySpace.Item);
		hash = Mix(hash, (ulong)itemId);
		return Finalize(hash);
	}

	public static ulong Stock(long swid, long siid)
	{
		ulong hash = Begin(KeySpace.Stock);
		hash = Mix(hash, (ulong)swid);
		hash = Mix(hash, (ulong)siid);
		return Finalize(hash);
	}

	public static ulong History(long hcid, long hdate)
	{
		ulong hash = Begin(KeySpace.History);
		hash = Mix(hash, (ulong)hcid);
		hash = Mix(hash, (ulong)hdate);
		return Finalize(hash);
	}

	public static ulong NewOrder(long nowid, long nodid, long nooid)
	{
		ulong hash = Begin(KeySpace.NewOrder);
		hash = Mix(hash, (ulong)nowid);
		hash = Mix(hash, (ulong)nodid);
		hash = Mix(hash, (ulong)nooid);
		return Finalize(hash);
	}

	public static ulong Order(long owid, long odid, long oid)
	{
		ulong hash = Begin(KeySpace.Order);
		hash = Mix(hash, (ulong)owid);
		hash = Mix(hash, (ulong)odid);
		hash = Mix(hash, (ulong)oid);
		return Finalize(hash);
	}

	public static ulong OrderLine(long olwId, long oldId, long oloId, long olNumber)
	{
		ulong hash = Begin(KeySpace.OrderLine);
		hash = Mix(hash, (ulong)olwId);
		hash = Mix(hash, (ulong)oldId);
		hash = Mix(hash, (ulong)oloId);
		hash = Mix(hash, (ulong)olNumber);
		return Finalize(hash);
	}

	private enum KeySpace : uint
	{
		Ycsb = 1,
		Warehouse = 2,
		District = 3,
		Customer = 4,
		Item = 5,
		Stock = 6,
		History = 7,
		NewOrder = 8,
		Order = 9,
		OrderLine = 10,
	}
}

public sealed unsafe class UnsafeConflictTracker : IDisposable
{
	private const float LoadFactor = 0.75f;
	private const int DefaultCapacity = 64;
	private const int InitialEntryCapacity = 4;

	private Bucket* _buckets;
	private int _bucketCapacity;
	private int _bucketCount;
	private int _resizeThreshold;
	private bool _disposed;

	public UnsafeConflictTracker(int estimatedCapacity)
	{
		if (estimatedCapacity <= 0)
		{
			estimatedCapacity = DefaultCapacity;
		}

		Initialize(estimatedCapacity);
	}

	public void Add(ulong keyHash, int rid, int txcIdx)
	{
		if (_disposed)
		{
			throw new ObjectDisposedException(nameof(UnsafeConflictTracker));
		}

		ref var bucket = ref LocateBucket(keyHash);
		bool isNew = bucket.Count == 0 && bucket.Capacity == 0 && bucket.Entries == null;

		AppendEntry(ref bucket, rid, txcIdx);

		if (isNew)
		{
			_bucketCount++;

			if (_bucketCount >= _resizeThreshold)
			{
				Resize();
			}
		}
	}

	public BucketEnumerator GetEnumerator() => new BucketEnumerator(_buckets, _bucketCapacity);

	public bool TryGetBucket(ulong key, out BucketView view)
	{
		if (_bucketCapacity == 0)
		{
			view = default;
			return false;
		}

		int mask = _bucketCapacity - 1;
		int index = (int)(key & (ulong)mask);
		int scanned = 0;

		while (scanned < _bucketCapacity)
		{
			Bucket* bucketPtr = _buckets + index;

			if (bucketPtr->Entries == null && bucketPtr->Capacity == 0)
			{
				break;
			}

			if (bucketPtr->Key == key && bucketPtr->Count > 0)
			{
				view = new BucketView(bucketPtr);
				return true;
			}

			index = (index + 1) & mask;
			scanned++;
		}

		view = default;
		return false;
	}

	public void Dispose()
	{
		Dispose(true);
		GC.SuppressFinalize(this);
	}

	~UnsafeConflictTracker()
	{
		Dispose(false);
	}

	private void Initialize(int estimatedCapacity)
	{
		_bucketCapacity = NextPowerOfTwo(Math.Max(DefaultCapacity, estimatedCapacity));
		_buckets = (Bucket*)NativeMemory.AllocZeroed((nuint)_bucketCapacity, (nuint)sizeof(Bucket));
		_bucketCount = 0;
		_resizeThreshold = (int)(_bucketCapacity * LoadFactor);
	}

	private ref Bucket LocateBucket(ulong keyHash)
	{
		if (_bucketCapacity == 0)
		{
			Initialize(DefaultCapacity);
		}

		int mask = _bucketCapacity - 1;
		int index = (int)(keyHash & (ulong)mask);

		while (true)
		{
			ref var bucket = ref GetBucketRef(index);

			if (bucket.Entries == null && bucket.Capacity == 0)
			{
				bucket.Key = keyHash;
				return ref bucket;
			}

			if (bucket.Key == keyHash)
			{
				return ref bucket;
			}

			index = (index + 1) & mask;
		}
	}

	private void Resize()
	{
		int newCapacity = _bucketCapacity << 1;
		Bucket* newBuckets = (Bucket*)NativeMemory.AllocZeroed((nuint)newCapacity, (nuint)sizeof(Bucket));

		for (int i = 0; i < _bucketCapacity; i++)
		{
			ref var bucket = ref GetBucketRef(i);
			if (bucket.Entries == null)
			{
				continue;
			}

			int mask = newCapacity - 1;
			int index = (int)(bucket.Key & (ulong)mask);

			while (true)
			{
				ref var target = ref Unsafe.Add(ref Unsafe.AsRef<Bucket>(newBuckets), index);

				if (target.Entries == null && target.Capacity == 0)
				{
					target = bucket;
					break;
				}

				index = (index + 1) & mask;
			}
		}

		NativeMemory.Free(_buckets);
		_buckets = newBuckets;
		_bucketCapacity = newCapacity;
		_resizeThreshold = (int)(_bucketCapacity * LoadFactor);
	}

	private static void AppendEntry(ref Bucket bucket, int rid, int txcIdx)
	{
		if (bucket.Capacity == 0)
		{
			bucket.Capacity = InitialEntryCapacity;
			bucket.Entries = (Entry*)NativeMemory.Alloc((nuint)bucket.Capacity, (nuint)sizeof(Entry));
		}
		else if (bucket.Count == bucket.Capacity)
		{
			int newCapacity = bucket.Capacity << 1;
			bucket.Entries = (Entry*)NativeMemory.Realloc(bucket.Entries, (nuint)newCapacity * (nuint)sizeof(Entry));
			bucket.Capacity = newCapacity;
		}

		bucket.Entries[bucket.Count].Rid = rid;
		bucket.Entries[bucket.Count].TxcIdx = txcIdx;
		bucket.Count++;
	}

	private void Dispose(bool disposing)
	{
		if (_disposed)
		{
			return;
		}

		if (_buckets != null)
		{
			for (int i = 0; i < _bucketCapacity; i++)
			{
				Bucket* bucket = _buckets + i;
				if (bucket->Entries != null)
				{
					NativeMemory.Free(bucket->Entries);
					bucket->Entries = null;
				}
			}

			NativeMemory.Free(_buckets);
			_buckets = null;
		}

		_bucketCapacity = 0;
		_bucketCount = 0;
		_resizeThreshold = 0;
		_disposed = true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ref Bucket GetBucketRef(int index)
	{
		return ref Unsafe.Add(ref Unsafe.AsRef<Bucket>(_buckets), index);
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

	[StructLayout(LayoutKind.Sequential)]
	internal struct Bucket
	{
		public ulong Key;
		public Entry* Entries;
		public int Count;
		public int Capacity;
	}

	[StructLayout(LayoutKind.Sequential)]
	public struct Entry
	{
		public int Rid;
		public int TxcIdx;
	}

	public readonly ref struct BucketView
	{
		private readonly Bucket* _bucket;

		internal BucketView(Bucket* bucket)
		{
			_bucket = bucket;
		}

		public ulong Key => _bucket->Key;

		public ReadOnlySpan<Entry> Entries => _bucket->Entries == null
			? ReadOnlySpan<Entry>.Empty
			: new ReadOnlySpan<Entry>(_bucket->Entries, _bucket->Count);
	}

	public ref struct BucketEnumerator
	{
		private readonly Bucket* _buckets;
		private readonly int _capacity;
		private int _index;

		internal BucketEnumerator(Bucket* buckets, int capacity)
		{
			_buckets = buckets;
			_capacity = capacity;
			_index = -1;
		}

		public BucketView Current
		{
			get
			{
				Bucket* bucket = _buckets + _index;
				return new BucketView(bucket);
			}
		}

		public bool MoveNext()
		{
			while (++_index < _capacity)
			{
				Bucket* bucket = _buckets + _index;
				if (bucket->Entries != null && bucket->Count > 0)
				{
					return true;
				}
			}

			return false;
		}
	}
}



