using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Net.Sockets;
using System.IO.Pipelines;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;
using System.Collections.Concurrent;
using DotNext;
using System.IO;
using ProtoBuf;
using System.Buffers;
using DotNext.Buffers;
using DotNext.Diagnostics;
using MemoryPack.Streaming;

namespace Minerva.Grpc_Client;


public class ClientResponseHandler : IReceivedRequestHandler
{

    ConcurrentDictionary<uint, TaskCompletionSource<TxResult>> _pendingRequests;

    public ClientResponseHandler(ConcurrentDictionary<uint, TaskCompletionSource<TxResult>> pendingRequests)
    {
        _pendingRequests = pendingRequests;
    }


    public void HandleMessageAsync(Stream stream, object message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message is TxResult txResult)
        {
            if (_pendingRequests.TryRemove(txResult.SeqId, out var tcs))
            {
                tcs.SetResult(txResult);
            }
            else
            {
                throw new InvalidOperationException($"No pending request found for SeqId {txResult.SeqId}");
            }
        }
        else
        {
            throw new InvalidOperationException($"Received unknown message type from server {message.GetType()}");
        }
    }
}





public class Client : IDisposable
{
    private string _serverAddr;
    private readonly int _serverPort;
    private readonly TcpClient _tcpClient;
    private Stream _networkStream;
    private CancellationTokenSource _cts = new();
    public ConcurrentDictionary<uint, TaskCompletionSource<TxResult>> PendingRequests = new();

    public Client(string serverAddress, int serverPort)
    {
        _serverAddr = serverAddress;
        _serverPort = serverPort;

        _tcpClient = new TcpClient
        {
            NoDelay = true
        };
    }

    public async Task ConnectAsync()
    {
        for (int i = 0; i < 300; i++)
        {
            try
            {
                await _tcpClient.ConnectAsync(_serverAddr, _serverPort, _cts.Token).ConfigureAwait(false);
                break;
            }
            catch (Exception)
            {
                Console.Write("\rConnecting to server {0}:{1}, attempt {2}/300", _serverAddr, _serverPort, i + 1);

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
            }
        }
        
        if (!_tcpClient.Connected)
        {
            throw new InvalidOperationException($"Failed to connect to server {_serverAddr}:{_serverPort} after 300 seconds");
        }


        _networkStream = _tcpClient.GetStream();
        
        var responseHandler = new ClientResponseHandler(PendingRequests);


        // Start background message processing
        var responseProcessingTask = Task.Run(async () =>
        {

            MessageProcessorGeneric<TxResult> messageProcessor = new(_networkStream, responseHandler);

            try
            {
                await messageProcessor.ProcessMessagesAsync(_cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error when processing: {ex.Message}, {ex.StackTrace}");
            }
            finally
            {
                await messageProcessor.CompleteAsync().ConfigureAwait(false);
            }

        }, _cts.Token);

    }

    public async Task SendMessageAsync(ClientRequest message)
    {

            var memoryOwner = ProtoMessageHelper.SerializeToMemoryOwner(message);

            await _networkStream.WriteAsync(memoryOwner.Memory, _cts.Token);
            
            memoryOwner.Dispose();
            

    }


    private static uint _nextSeqId = 1;
    public async Task<TxInfo> SendTransaction(TxInfo tx, BenchmarkResult result, int waitTime = 60)
    {
        ClientRequest request = tx.Query;

        request.SeqId = Interlocked.Increment(ref _nextSeqId);

        Interlocked.Increment(ref result.SentTx);

        tx.StartTime = DateTime.Now;
        result.AllTxs.Add(tx);
        try
        {
            var tcs = new TaskCompletionSource<TxResult>();
            PendingRequests[request.SeqId] = tcs;
            await SendMessageAsync(request);
            var res = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(waitTime), _cts.Token);
            tx.Result = res;
            tx.EndTime = DateTime.Now;
        }
        catch (OperationCanceledException)
        {
            tx.Result = new TxResult
            {
                Executed = false,
            };
        }
        catch (TimeoutException)
        {
            tx.Result = new TxResult
            {
                Executed = false,
            };
        }
        catch (Exception e)
        {
            var errMsg = e.Message;
            Interlocked.Increment(ref result.FailedTx);
            tx.Result = new TxResult
            {
                Executed = false,
                TxResultStr = $"Failed to send Tx {errMsg}"
            };

            Console.WriteLine($"Error running transaction to server {_serverAddr}:{_serverPort}: {errMsg} {e.StackTrace}");
        }



        return tx;

    }



    public override string ToString()
    {
        return $"Server address {_serverAddr}:{_serverPort}";
    }

    public void Dispose()
    {

        _cts.Cancel();
        _cts.Dispose();
        _networkStream.Close();
        _tcpClient.Close();
        GC.SuppressFinalize(this);

    }
}


public class Cluster
{
    public List<Client> Servers { get; set; } = [];

    public Cluster(BenchmarkConfig config)
    {

        foreach (var server in config.Servers)
        {
            string[] parts = server.Split(':');
            if (parts.Length != 2 || !int.TryParse(parts[1], out int port))
            {
                throw new ArgumentException($"Invalid server address format: {server}");
            }

            Servers.Add(new Client(parts[0], port));
        }

        foreach (var server in Servers)
        {
            try
            {
                server.ConnectAsync().Wait();
            }
            catch (Exception)
            {
                throw new InvalidOperationException($"Failed to connect to server: {server}");
            }
        }
    }


    public void Dispose()
    {
        Parallel.ForEach(Servers, server =>
            server.Dispose());
    }
}