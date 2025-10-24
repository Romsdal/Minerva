using System;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;
using ProtoBuf;
using System.Net;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using System.IO.Pipelines;
using System.Buffers;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Network;


public class Server
{
    private readonly IPEndPoint _clientConnectionEP;
    private readonly IPEndPoint _serverConnectionEP;
    private readonly IPEndPoint _batchPropagationEP;

    private TcpListener _clientListener;
    private TcpListener _serverListener;
    private TcpListener _batchPropagationListener;

    private readonly CancellationTokenSource _cts = new();
    private readonly IReceivedRequestHandler _serverRequestHandler;
    private readonly IReceivedRequestHandler _clientRequestHandler;
    private readonly IReceivedRequestHandler _batchRequestHandler;

    private readonly ILogger _logger = LoggerManager.GetLogger();


    public Server(int port, IReceivedRequestHandler requestHandler, IReceivedRequestHandler clientRequestHandler, IReceivedRequestHandler batchRequestHandler)
    {
        _clientConnectionEP = new IPEndPoint(IPAddress.Any, port);
        _serverConnectionEP = new IPEndPoint(IPAddress.Any, port + 10000);
        _batchPropagationEP = new IPEndPoint(IPAddress.Any, port + 10001);
        _serverRequestHandler = requestHandler;
        _clientRequestHandler = clientRequestHandler;
        _batchRequestHandler = batchRequestHandler;
    }

    public void Start()
    {
        _serverListener = new TcpListener(_serverConnectionEP);
        _serverListener.Start();

        _clientListener = new TcpListener(_clientConnectionEP);
        _clientListener.Start();

        _batchPropagationListener = new TcpListener(_batchPropagationEP);
        _batchPropagationListener.Start();

        Console.WriteLine($"Server started on {_serverConnectionEP}");

        // Accept connections in a loop
        Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var tcpClient = await _serverListener.AcceptTcpClientAsync();
                    tcpClient.NoDelay = true;

                    // Handle each client connection concurrently
                    _ = Task.Run(async () => await HandleServerMessageAsync(tcpClient, _cts.Token));
                }
                catch (ObjectDisposedException)
                {
                    // Server is shutting down
                    break;
                }
                catch (OperationCanceledException)
                {
                    // Server is shutting down
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error accepting client: {ex.Message}", ex.Message);
                }
            }
        });


        // Accept connections in a loop
        Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var tcpClient = await _clientListener.AcceptTcpClientAsync();
                    tcpClient.NoDelay = true;
                    // Handle each client connection concurrently
                    _ = Task.Run(async () => await HandleClientMessageAsync(tcpClient, _cts.Token));
                }
                catch (ObjectDisposedException)
                {
                    // Server is shutting down
                    break;
                }
                catch (OperationCanceledException)
                {
                    // Server is shutting down
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error accepting client: {ex.Message}", ex.Message);
                }
            }
        });


        Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var tcpClient = await _batchPropagationListener.AcceptTcpClientAsync();

                    // Handle each client connection concurrently
                    _ = Task.Run(async () => await HandleBatchMessageAsync(tcpClient, _cts.Token));
                }
                catch (ObjectDisposedException)
                {
                    // Server is shutting down
                    break;
                }
                catch (OperationCanceledException)
                {
                    // Server is shutting down
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error accepting client: {ex.Message}", ex.Message);
                }
            }
        });

    }



    private async Task HandleServerMessageAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            var stream = client.GetStream();
            //var temp = client.GetStream();
            //var stream = new MonitoredNetworkStream(temp, BandwidthMonitorGlobal.Instance);

            MessageProcessor messageProcessor = new(stream, _serverRequestHandler);

            try
            {
                await messageProcessor.ProcessMessagesAsync(ct);

            }
            catch (Exception ex)
            {
                _logger.LogError("Error handling client of {Address}:{Port} - {ex.Message} {stackTrace}", ((IPEndPoint)client.Client.RemoteEndPoint).Address, ((IPEndPoint)client.Client.RemoteEndPoint).Port, ex.Message, ex.StackTrace);
            }
            finally
            {
                await messageProcessor.CompleteAsync();
            }
        }
    }


    private async Task HandleClientMessageAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            var stream = client.GetStream();
            // var temp = client.GetStream();
            // var stream = new MonitoredNetworkStream(temp, BandwidthMonitorGlobal.Instance);

            MessageProcessorGeneric<ClientRequest> messageProcessor = new(stream, _clientRequestHandler);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await messageProcessor.ProcessMessagesAsync(ct);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError("Error handling client of {Address}:{Port} - {ex.Message} {stackTrace}", ((IPEndPoint)client.Client.RemoteEndPoint).Address, ((IPEndPoint)client.Client.RemoteEndPoint).Port, ex.Message, ex.StackTrace);
            }

        }
    }


    private async Task HandleBatchMessageAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            var stream = client.GetStream();
            //var temp = client.GetStream();
            //var stream = new MonitoredNetworkStream(temp, BandwidthMonitorGlobal.Instance);

            MessageProcessorGeneric<BatchMsg> messageProcessor = new(stream, _batchRequestHandler);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await messageProcessor.ProcessMessagesAsync(ct);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError("Error handling client of {Address}:{Port} - {ex.Message} {stackTrace}", ((IPEndPoint)client.Client.RemoteEndPoint).Address, ((IPEndPoint)client.Client.RemoteEndPoint).Port, ex.Message, ex.StackTrace);
            }

        }
    }




    public void Stop()
    {
        _cts.Cancel();
        _serverListener?.Stop();
    }
}


