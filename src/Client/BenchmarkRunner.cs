
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.Grpc_Client;


public class BenchmarkRunner
{
    private IBenchmarkWorkload _workload;
    private string[] _serverAddresses;
    private BenchmarkConfig _config;
    private BenchmarkResult _result;
    private List<Thread> _clientThreads;
    private List<Client> _workerClients; // One client per worker thread
    private Thread _watchdogThread;


    public BenchmarkRunner(BenchmarkConfig config, string[] serverAddresses, IBenchmarkWorkload workload)
    {

        Console.WriteLine("Configuring benchmark: {0}", config);


        _config = config;
        _serverAddresses = serverAddresses;
        _workload = workload;
        _result = new BenchmarkResult();
        _clientThreads = new List<Thread>();
        _workerClients = new List<Client>();

        // Initialize one client per worker thread
        InitializeClients();

    }

    private void InitializeClients()
    {
        int totalClients = _config.Clients * _serverAddresses.Length;
        Console.WriteLine("Initializing {0} client threads with round-robin server assignment", totalClients);

        if (totalClients == 0)
        {
            Console.WriteLine("No clients were configured");
            return;
        }

        var connectionTasks = new Task[totalClients];
        var clients = new Client[totalClients];

        for (int threadId = 0; threadId < totalClients; threadId++)
        {
            string serverAddr = _serverAddresses[threadId % _serverAddresses.Length];
            var parts = serverAddr.Split(':');
            string host = parts[0];
            int port = int.Parse(parts[1]);

            var client = new Client(host, port);
            clients[threadId] = client;

            connectionTasks[threadId] = ConnectClientAsync(threadId, serverAddr, client);
        }

        Task.WhenAll(connectionTasks).Wait();

        _workerClients.Clear();
        _workerClients.AddRange(clients);
        async Task ConnectClientAsync(int threadId, string serverAddr, Client client)
        {
            try
            {
                await client.ConnectAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Thread {0}: Failed to connect to server {1}: {2}", threadId, serverAddr, ex.Message);
                client.Dispose();
                clients[threadId] = null;
            }
        }

        Thread.Sleep(10000); // Wait a bit for servers to be ready
        Console.WriteLine("Client initialization complete");
    }



    public void LoadDatabase()
    {
        Console.WriteLine("Loading databases");
        
        // Create and start client threads for loading
        for (int i = 0; i < Math.Min(_serverAddresses.Length * 2, _workerClients.Count); i++)
        {
            int threadId = i;
            Client client = _workerClients[threadId];
            Thread clientThread = new Thread(() => LoadDatabaseWorker(threadId, client))
            {
                Name = $"LoadClient-{threadId}"
            };
            _clientThreads.Add(clientThread);
            clientThread.Start();
        }

        // Wait for all load threads to complete
        foreach (Thread thread in _clientThreads)
        {
            thread.Join();
        }

        Console.WriteLine("\nLoad Complete. Total load transactions: {0}", _result.LoadTx);
        
        // Get stats after loading
        //GetStats();
        
        // Clear threads for benchmark phase
        _clientThreads.Clear();
    }

    private void LoadDatabaseWorker(int threadId, Client client)
    {
        if (client == null)
        {
            Console.WriteLine("Thread {0}: No server connection available", threadId);
            return;
        }

        while (!_workload.IsLoadComplete)
        {
            if (_workload.GetNextLoadDataQuery(out var query))
            {
                try
                {
                    // Send to assigned server
                    client.SendTransaction(query, _result).Wait();
                    
                    if (threadId == 0) // Only one thread shows progress
                    {
                        Console.Write("\rLoad progress: {0:P2}", _workload.Progress);
                    }
                    
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Thread {0}: Load transaction failed: {1}", threadId, ex.Message);
                }
            }
            else
            {
                break;
            }
        }
    }

    volatile bool _benchmarkRunning = false;
    public BenchmarkResult RunBenchmark()
    {
        if (_config.Duration == 0)
            return _result;

        Console.WriteLine("Starting benchmark with {0} clients per server for {1} seconds", _config.Clients, _config.Duration);
        
        CancellationTokenSource runningCts = new();
        

        
        // Create and start client threads for benchmark
        for (int i = 0; i < _config.Clients * _serverAddresses.Length; i++)
        {
            int threadId = i;
            Client client = _workerClients[threadId];
            Thread clientThread = new Thread(() => BenchmarkWorker(threadId, client, runningCts.Token))
            {
                Name = $"BenchmarkClient-{threadId}"
            };
            _clientThreads.Add(clientThread);
           
        }

        var startTime = DateTime.Now;
        var endTime = startTime.AddSeconds(_config.Duration);
        _benchmarkRunning = true;
        Parallel.ForEach(_clientThreads, thread => thread.Start());

        while (DateTime.Now < endTime)
        {
            Thread.Sleep(500);
            Console.Write("\rTime remaining: {0} seconds", (int)(endTime - DateTime.Now).TotalSeconds);
        }

        _benchmarkRunning = false;

        // Wait for all threads to complete
        foreach (Thread thread in _clientThreads)
        {
            thread.Join();
        }


        Thread.Sleep(2000); // Wait a bit for any final transactions to complete
        runningCts.Cancel();
        
        // Get stats after benchmark
        //GetStats().Wait();

        // Dispose all clients
        Dispose();
        
        return _result;
    }

    private async void BenchmarkWorker(int threadId, Client client, CancellationToken cancellationToken)
    {
        if (client == null)
        {
            Console.WriteLine("Thread {0}: No server connection available", threadId);
            return;
        }

        while (_benchmarkRunning)
        {
            if (_workload.GetNextQuery(out var query))
            {
                    // Send to assigned server
                    await client.SendTransaction(query, _result);
                    //Thread.Sleep(_txSleepInterval);
            }
            else
            {
                // No more queries available
                break;
            }
        }
    }

    private async Task GetStats()
    {
        // Get stats from each server exactly once using the first x clients 
        // (where x is the number of servers) since clients are assigned round-robin
        int numServersToQuery = Math.Min(_serverAddresses.Length, _workerClients.Count);

        Console.WriteLine("Getting stats from {0} servers:", numServersToQuery);

        for (int i = 0; i < numServersToQuery; i++)
        {
            Client statsClient = _workerClients[i];

            if (statsClient != null)
            {
                try
                {
                    TxInfo req = new()
                    {
                        Query = new ClientRequest()
                        {
                            Type = QueryType.Stats
                        }
                    };

                    var res = await statsClient.SendTransaction(req, _result);
                    Console.WriteLine("Server {0} stats: {1}", _serverAddresses[i], res.Result.TxResultStr);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failed to get stats from server {0}: {1} {2}", _serverAddresses[i], ex.Message, ex.StackTrace);
                }
            }
            else
            {
                Console.WriteLine("No client available for server {0}", _serverAddresses[i]);
            }
        }
    }


    public async Task SaveDatabase()
    {
        // Get stats from each server exactly once using the first x clients 
        // (where x is the number of servers) since clients are assigned round-robin
        int numServersToQuery = Math.Min(_serverAddresses.Length, _workerClients.Count);

        Console.WriteLine("Saving database for {0} servers:", numServersToQuery);

        for (int i = 0; i < numServersToQuery; i++)
        {
            Client statsClient = _workerClients[i];

            if (statsClient != null)
            {
                try
                {
                    TxInfo req = new()
                    {
                        Query = new ClientRequest()
                        {
                            Type = QueryType.SaveState
                        }
                    };

                    var res = await statsClient.SendTransaction(req, _result, 3600);
                    if (!res.Result.Executed)
                    {
                        throw new Exception("Save state command failed: " + res.Result.TxResultStr);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failed to save state at server {0}: {1} {2}", _serverAddresses[i], ex.Message, ex.StackTrace);
                }
            }
            else
            {
                Console.WriteLine("No client available for server {0}", _serverAddresses[i]);
            }
        }

        if (_workload is YCSB saveWorkload)
        {
            saveWorkload.SaveKeys();
            Console.WriteLine("YCSB keys saved");
        }
    }


    public void Dispose()
    {
        Console.WriteLine("Closing Servers");
        for (int i = 0; i < Math.Min(_serverAddresses.Length, _workerClients.Count);i++)
        {
            Client stopClient = _workerClients[i];

            ClientRequest request = new()
            {
                Type = QueryType.Stop
            };

            // Send stop request synchronously
            stopClient.SendMessageAsync(request);
        }
        
        for (int i = 0; i < _workerClients.Count; i++)
        {
            if (_workerClients[i] != null)
            {
                try
                {
                    _workerClients[i].Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Thread {0}: Error disposing client: {1}", i, ex.Message);
                }
            }
        }
        
        _workerClients.Clear();
        Console.WriteLine("All client connections disposed");
    }
}