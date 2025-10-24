using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_server.Interface;
using Minerva.DB_Server.Interface;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Raft;
using Minerva.DB_Server.Storage;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server;

public enum InterfaceType
{
    CLI,
    GRPC,
    Test
}


public class MinervaService : IDisposable
{
    private MinervaConfig _config;
    private PersistentStorage _persistedDb;
    private IQueryParser _queryParser;
    private MinervaTxOCCExecutor _minervaTxOCCExecutor;
    private DeterministicExecutor _deterministicExecutor;
    private Cluster _cluster;
    private ILogConnections _logConnections;
    private GlobalLogManager _logManager;
    private LogReceiveRequestHandler _logReceiveHandler;
    private ProtoClientInterface _clientInterface;
    private Server _server;
    private RaftManager _raftManager;
    private QueryHandler _queryHandler;
    private IDBInterface _interface;

    private readonly CancellationTokenSource _globalCts = new();
    private readonly ILogger _logger = LoggerManager.GetLogger();


    public MinervaService(MinervaConfig config, NodeInfo[] nodes)
    {
        _config = config;
        _cluster = Cluster.CreateFromConfig(nodes, _globalCts.Token);
        if (_config.ReplicaPriority.Length != _cluster.Nodes.Length)
        {
            throw new ArgumentException("ReplicaPriority length must match the number of nodes in the cluster.");
        }

        _persistedDb = new PersistentStorage();

        if (!string.IsNullOrEmpty(_config.ReadStorage))
        {
            _persistedDb.LoadStorageFromDisk(_config.ReadStorage, _config.DatabaseToLoad).Wait();
            _logger.LogInformation("Loaded storage from {Path}", _config.ReadStorage);
        }


        _queryParser = new BenchmarkQueryParser();

        _minervaTxOCCExecutor = new(_queryParser, _persistedDb);
        _deterministicExecutor = new(_queryParser, _persistedDb, _cluster.SelfNode.Id);

        _raftManager = new RaftManager(_cluster.Nodes);
        _logConnections = new LogConnections(_cluster);

        _logManager = new GlobalLogManager(config, _cluster, _persistedDb, _minervaTxOCCExecutor, _deterministicExecutor, _logConnections, _raftManager);

        _queryHandler = new QueryHandler(_minervaTxOCCExecutor, _logManager, _persistedDb);

        _logReceiveHandler = new LogReceiveRequestHandler(_logManager);
        _clientInterface = ProtoClientInterface.GetInstance(_queryHandler);


        var receivedRequestHandler = new ServerReceivedRequestHandler(_cluster, _logReceiveHandler);
        var clientRequestHandler = new ReceivedClientRequestHandler(_clientInterface);
        var batchRequestHandler = new ServerReceivedBatchHandler(_logReceiveHandler);
        _server = new Server(_cluster.SelfNode.Port, receivedRequestHandler, clientRequestHandler, batchRequestHandler);

        _logManager.TxCompleteHandler = _queryHandler.NotifyTransactionCommitted;

        MessageTypeResolver.ProtoBufPrepareSerializer();
        //BandwidthMonitorGlobal.Instance = new BandwidthMonitor(); 
    }

    
    public void Init()
    {
        _server.Start();
        Thread.Sleep(1000);
        _cluster.Initialize();
        if (!_cluster.IsInitialized())
        {
            throw new InvalidOperationException("Cluster initialization failed - not all nodes connected");
        }
        
        _raftManager.Init().Wait();
        Thread.Sleep(1000); // give some time for the raft node to stabilize
        _logManager.Configure(_raftManager.IsCurrentNodeLeader);
        _logManager.RunTicker();

        _logger.LogInformation("MinervaService initialized, current node id: {NodeId}, host: {Host}, port: {Port}, leader: {IsLeader}",
            _cluster.SelfNode.Id, _cluster.SelfNode.Address, _cluster.SelfNode.Port, _logManager.IsCoordinator);
    }

    public void StartInterface(InterfaceType interfaceType = InterfaceType.CLI)
    {
        _interface = interfaceType switch
        {
            InterfaceType.CLI => CLIInterface.GetInstance(_queryHandler),
            InterfaceType.GRPC => _clientInterface,
            InterfaceType.Test => TestInterface.GetInstance(_queryHandler),
            _ => throw new ArgumentOutOfRangeException(nameof(interfaceType), interfaceType, null),
        };
        _logger.LogInformation("Starting Minerva interface...");
        _interface.Start();
        _logger.LogInformation("Minerva interface stopped");
        _logger.LogWarning("Final stats:\n{Stats}", Stats.GetStats());
    }

    public void StopInterface()
    {
        _interface.Stop();
    }

    public Dictionary<string, string> ServiceStatus()
    {
        return new Dictionary<string, string>
        {
            { "NodeId", _cluster.SelfNode.Id.ToString() },
            { "Host", _cluster.SelfNode.Address },
            { "Port", _cluster.SelfNode.Port.ToString() },
            { "Coordinator", _logManager.IsCoordinator ? "Yes" : "No" },
            { "EpochId", _logManager.CurrentEpochId.ToString() },
        };
    }


    public void Dispose()
    {   _globalCts.Cancel();
        _logManager.Stop();

        Thread.Sleep(500);

        _cluster.Dispose();
        _server.Stop();
        _globalCts.Dispose();

        GC.SuppressFinalize(this);
        _logger.LogInformation("MinervaService stopped");
    }


    // ===== Test Only Methods =====
    [Obsolete("Test only method")]
    public TestInterface GetInterface()
    {
        return (TestInterface) _interface;
    }
}