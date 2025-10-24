using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server;
using Minerva.DB_Server.Benchmarks;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.Raft;

// A mock raft log to store entries after the consensus part.
public class RaftLog
{
    public List<List<int>> Log = [];
    private readonly Dictionary<int, List<int>> _pendingLogs = new();
    private int _nextExpectedEpochId = 0;
    private readonly ILogger _logger = LoggerManager.GetLogger();

    public ReaderWriterLockSlim LogLock = new();


    public RaftLog()
    {

    }


    public void AppendLog(List<int> indices, int epochId)
    {
        ////_logger.LogTrace("[RaftNode] Appending log for epoch {EpochId} with indices {Indices}", epochId, string.Join(", ", indices));
        LogLock.EnterWriteLock();
        try
        {
            // Store the log entry temporarily
            _pendingLogs[epochId] = indices;

            // Process logs in order starting from the next expected epochId
            while (_pendingLogs.ContainsKey(_nextExpectedEpochId))
            {
                var logIndices = _pendingLogs[_nextExpectedEpochId];

                // Add the indices to the log directly
                Log.Add(logIndices);

                // Remove the processed log from pending
                _pendingLogs.Remove(_nextExpectedEpochId);

                // Move to next expected epochId
                _nextExpectedEpochId++;
            }
        }
        finally
        {
            LogLock.ExitWriteLock();
        }
    }
}