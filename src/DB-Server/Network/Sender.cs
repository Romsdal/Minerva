using System;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ProtoBuf;

namespace Minerva.DB_Server.Network;


public class ServerSender
{
    private string _serverAddr;
    private readonly int _serverPort;
    private readonly int _batchPort;
    private readonly TcpClient _tcpClient;
    private Stream _networkStream;


    private readonly TcpClient _batchSenderClient;
    private Stream _batchSenderStream;

    private readonly TcpClient _commitSenderClient;
    private Stream _commitSenderStream;
    private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1, 1);


    private CancellationTokenSource _cts = new();
    private readonly ILogger _logger = LoggerManager.GetLogger();


    public ServerSender(string serverAddress, int serverPort)
    {
        _serverAddr = serverAddress;
        _serverPort = serverPort + 10000;
        _batchPort = serverPort + 10001;

        _tcpClient = new TcpClient()
        {
            NoDelay = true
        };

        _batchSenderClient = new TcpClient();
        
        _commitSenderClient = new TcpClient()
        {
            NoDelay = true,
        };

    }

    public async Task ConnectAsync()
    {
        if (!_tcpClient.Connected)
        {
            await _tcpClient.ConnectAsync(_serverAddr, _serverPort, _cts.Token);
            await _commitSenderClient.ConnectAsync(_serverAddr, _serverPort, _cts.Token);
            await _batchSenderClient.ConnectAsync(_serverAddr, _batchPort, _cts.Token);
        }
        _logger.LogInformation("Connected to server {Address}:{Port} at port {Port}", _serverAddr, _serverPort, _tcpClient.Client.LocalEndPoint);
        _networkStream = _tcpClient.GetStream();
        //_networkStream = Stream.Synchronized(_networkStream);

        _batchSenderStream = _batchSenderClient.GetStream();
        
        _commitSenderStream = _commitSenderClient.GetStream();



    }

    public async Task SendMessage<T>(T message, int streamToUse = 0)
    {
        using var memoryOwner = ProtoMessageHelper.SerializeToMemoryOwnerWithFieldNumber(message);
        await SendMessageBytes(memoryOwner.Memory, streamToUse);
    }


    // streamToUse: 0 - normal, 1 - batch, 2 - commit
    public async Task SendMessageBytes(ReadOnlyMemory<byte> message, int streamToUse = 0)
    {
        var stream = streamToUse switch
        {
            0 => this._networkStream,
            1 => this._batchSenderStream,
            2 => this._commitSenderStream,
            _ => throw new ArgumentOutOfRangeException(nameof(streamToUse), "Invalid streamToUse value. Must be 0, 1, or 2.")
        };

        await _writeLock.WaitAsync();
        try
        {
            await stream.WriteAsync(message, _cts.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError("Failed to send message to server {Address}:{Port} - {ex.Message} {stacktrace}", _serverAddr, _serverPort, ex.Message, ex.StackTrace);
        }
        finally
        {
            _writeLock.Release();
        }
    }







    public void Dispose()
    {
        _cts.Cancel();
        _networkStream?.Dispose();
        _tcpClient?.Close();
    }
}