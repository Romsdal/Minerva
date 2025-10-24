using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Minerva;


// public static class BandwidthMonitorGlobal
// {
//     public static BandwidthMonitor Instance;
// }

/// <summary>
/// Monitors and reports bandwidth usage for incoming and outgoing network traffic
/// </summary>
public class BandwidthMonitor : IDisposable
{
    private long _totalBytesReceived = 0;
    private long _totalBytesSent = 0;
    private long _lastBytesReceived = 0;
    private long _lastBytesSent = 0;
    private readonly Timer _reportTimer;
    private readonly Stopwatch _stopwatch;
    private readonly object _lock = new object();

    public long TotalBytesReceived => _totalBytesReceived;
    public long TotalBytesSent => _totalBytesSent;

    public BandwidthMonitor()
    {
        _stopwatch = Stopwatch.StartNew();
        _reportTimer = new Timer(ReportBandwidth, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        Console.WriteLine("[BANDWIDTH] Monitoring started - reporting every second");
    }

    /// <summary>
    /// Record bytes received from network
    /// </summary>
    public void RecordBytesReceived(long bytes)
    {
        Interlocked.Add(ref _totalBytesReceived, bytes);
    }

    /// <summary>
    /// Record bytes sent to network
    /// </summary>
    public void RecordBytesSent(long bytes)
    {
        Interlocked.Add(ref _totalBytesSent, bytes);
    }

    private void ReportBandwidth(object? state)
    {
        lock (_lock)
        {
            var currentReceived = _totalBytesReceived;
            var currentSent = _totalBytesSent;
            var elapsed = _stopwatch.Elapsed.TotalSeconds;

            // Calculate bandwidth for the last second
            var receivedDelta = currentReceived - _lastBytesReceived;
            var sentDelta = currentSent - _lastBytesSent;

            var receivedMBps = (receivedDelta / 1024.0 / 1024.0);
            var sentMBps = (sentDelta / 1024.0 / 1024.0);

            // Calculate total averages
            var totalReceivedMBps = elapsed > 0 ? (currentReceived / 1024.0 / 1024.0) / elapsed : 0;
            var totalSentMBps = elapsed > 0 ? (currentSent / 1024.0 / 1024.0) / elapsed : 0;

            Console.WriteLine($"[BANDWIDTH] ↓ {receivedMBps:F2} MB/s | ↑ {sentMBps:F2} MB/s | " +
                            $"Total: ↓ {currentReceived / 1024.0 / 1024.0:F1} MB ({totalReceivedMBps:F2} avg) | " +
                            $"↑ {currentSent / 1024.0 / 1024.0:F1} MB ({totalSentMBps:F2} avg)");

            _lastBytesReceived = currentReceived;
            _lastBytesSent = currentSent;
        }
    }

    public void Dispose()
    {
        _reportTimer?.Dispose();
        _stopwatch?.Stop();
        Console.WriteLine($"[BANDWIDTH] Monitoring stopped - Total: ↓ {_totalBytesReceived / 1024.0 / 1024.0:F1} MB, ↑ {_totalBytesSent / 1024.0 / 1024.0:F1} MB");
    }
}

/// <summary>
/// Wrapper around NetworkStream that tracks bandwidth usage
/// </summary>
public class MonitoredNetworkStream : Stream
{
    private readonly NetworkStream _baseStream;
    private readonly BandwidthMonitor _monitor;

    public MonitoredNetworkStream(NetworkStream baseStream, BandwidthMonitor monitor)
    {
        _baseStream = baseStream;
        _monitor = monitor;
    }

    public override bool CanRead => _baseStream.CanRead;
    public override bool CanSeek => _baseStream.CanSeek;
    public override bool CanWrite => _baseStream.CanWrite;
    public override long Length => _baseStream.Length;
    public override long Position 
    { 
        get => _baseStream.Position; 
        set => _baseStream.Position = value; 
    }

    public override void Flush() => _baseStream.Flush();

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bytesRead = _baseStream.Read(buffer, offset, count);
        if (bytesRead > 0)
            _monitor.RecordBytesReceived(bytesRead);
        return bytesRead;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var bytesRead = await _baseStream.ReadAsync(buffer, offset, count, cancellationToken);
        if (bytesRead > 0)
            _monitor.RecordBytesReceived(bytesRead);
        return bytesRead;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var bytesRead = await _baseStream.ReadAsync(buffer, cancellationToken);
        if (bytesRead > 0)
            _monitor.RecordBytesReceived(bytesRead);
        return bytesRead;
    }

    public override long Seek(long offset, SeekOrigin origin) => _baseStream.Seek(offset, origin);

    public override void SetLength(long value) => _baseStream.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count)
    {
        _baseStream.Write(buffer, offset, count);
        _monitor.RecordBytesSent(count);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _baseStream.WriteAsync(buffer, offset, count, cancellationToken);
        _monitor.RecordBytesSent(count);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _baseStream.WriteAsync(buffer, cancellationToken);
        _monitor.RecordBytesSent(buffer.Length);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _baseStream?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await _baseStream.DisposeAsync();
        await base.DisposeAsync();
    }
}