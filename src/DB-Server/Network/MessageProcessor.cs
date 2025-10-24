using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using DotNext.Buffers;
using DotNext.Threading.Tasks;
using MemoryPack;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server.Network.Protos;
using ProtoBuf;

namespace Minerva.DB_Server.Network;

public class MessageProcessor
{

    private readonly PipeReader _reader;
    private readonly Stream _stream;
    private readonly IReceivedRequestHandler _receivedRequestHandler;
    private readonly ILogger _logger = LoggerManager.GetLogger();

    public MessageProcessor(Stream stream, IReceivedRequestHandler requestHandler)
    {
        _stream = stream;
        _reader = PipeReader.Create(stream);
        _receivedRequestHandler = requestHandler;
    }

    /// <summary>
    /// Process messages from the pipeline until cancellation is requested
    /// </summary>
    public async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    break;

                var position = buffer.Start;
                var consumed = position;

                try
                {
                    while (TryReadMessage(ref buffer, ref position, out var message))
                    {
                        consumed = position;

                        // Handle the message through the callback
                        if (message != null && !cancellationToken.IsCancellationRequested)
                        {

                            _ = Task.Run(() => _receivedRequestHandler.HandleMessageAsync(_stream, message), cancellationToken);
                        }
                    }
                }
                finally
                {
                    _reader.AdvanceTo(consumed, buffer.End);
                }

                if (result.IsCompleted)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing messages: {ex.Message}");
        }
    }

    /// <summary>
    /// Try to read a complete message from the buffer.
    /// So apparently, protobuf-net deserializes from network stream is very slow.
    /// </summary>
    private static bool TryReadMessage(ref ReadOnlySequence<byte> buffer, ref SequencePosition position, out object message)
    {
        message = null;
        var reader = new SequenceReader<byte>(buffer.Slice(position));

        // Read the length prefix (4 bytes)
        if (!reader.TryReadBigEndian(out int length))
        {
            return false;
        }

        if (length < 1)
        {
            Console.WriteLine($"Invalid message length: {length}");
            return false;
        }

        // Check if we have enough data for the complete message (length + field number)
        if (reader.Remaining < length)
            return false;

        // Read the field number (1 byte)
        if (!reader.TryRead(out byte fieldNumber))
            return false;

        // Adjust length for the actual protobuf data (excluding field number)
        int protobufLength = length - 1;

        // Read the protobuf message
        var messageBuffer = buffer.Slice(reader.Position, protobufLength);

        try
        {
            // Resolve the message type from field number
            Type messageType = MessageTypeResolver.ResolveType(fieldNumber);

            // Deserialize directly from ReadOnlySequence - protobuf-net handles single/multi-segment automatically
            //message = Serializer.NonGeneric.Deserialize(messageType, messageBuffer);
            message = MemoryPackSerializer.Deserialize(messageType, messageBuffer);

            reader.Advance(protobufLength);
            position = reader.Position;
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Complete the pipeline reader
    /// </summary>
    public async Task CompleteAsync()
    {
        await _reader.CompleteAsync();
    }

    public void Dispose()
    {
        _reader?.Complete();
    }
}


public class MessageProcessorGeneric<T>
{

    private readonly PipeReader _reader;
    private readonly Stream _stream;
    private readonly IReceivedRequestHandler _receivedRequestHandler;
    private readonly ILogger _logger = LoggerManager.GetLogger();

    public MessageProcessorGeneric(Stream stream, IReceivedRequestHandler requestHandler)
    {
        _stream = stream;
        _reader = PipeReader.Create(stream);
        _receivedRequestHandler = requestHandler;
    }

    /// <summary>
    /// Process messages from the pipeline until cancellation is requested
    /// </summary>
    public async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _reader.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    break;

                var position = buffer.Start;
                var consumed = position;

                try
                {
                    while (TryReadMessage(ref buffer, ref position, out var message))
                    {
                        consumed = position;

                        // Handle the message through the callback
                        if (message != null && !cancellationToken.IsCancellationRequested)
                        {

                            _ = Task.Run(() => _receivedRequestHandler.HandleMessageAsync(_stream, message), cancellationToken);
                        }
                    }
                }
                finally
                {
                    _reader.AdvanceTo(consumed, buffer.End);
                }

                if (result.IsCompleted)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing messages: {ex.Message}");
        }
    }

    /// <summary>
    /// Try to read a complete message from the buffer
    /// </summary>
    private static bool TryReadMessage(ref ReadOnlySequence<byte> buffer, ref SequencePosition position, out T message)
    {
        message = default;
        var reader = new SequenceReader<byte>(buffer.Slice(position));

        // Read the length prefix (4 bytes)
        if (!reader.TryReadBigEndian(out int length))
        {
            return false;
        }

        if (length < 1)
        {
            Console.WriteLine($"Invalid message length: {length}");
            return false;
        }

        // Check if we have enough data for the complete message (length + field number)
        if (reader.Remaining < length)
            return false;

        // Adjust length for the actual protobuf data (excluding field number)
        int protobufLength = length;

        // Read the protobuf message
        var messageBuffer = buffer.Slice(reader.Position, protobufLength);
        
        try
        {
            // Deserialize directly from ReadOnlySequence - protobuf-net handles single/multi-segment automatically
            //message = Serializer.Deserialize<T>(messageBuffer);
            message = MemoryPackSerializer.Deserialize<T>(messageBuffer);

            reader.Advance(protobufLength);
            position = reader.Position;
            return true;
        }
        catch
        {
            return false;
        }
    }




    /// <summary>
    /// Complete the pipeline reader
    /// </summary>
    public async Task CompleteAsync()
    {
        await _reader.CompleteAsync();
    }

    public void Dispose()
    {
        _reader?.Complete();
    }
}



public static class ProtoMessageHelper
{
    public static MemoryOwner<byte> SerializeToMemoryOwnerWithFieldNumber<T>(T message)
    {
        // Get the field number for the message type
        int fieldNumber = MessageTypeResolver.ResolveFieldNumber<T>();

        // Use PoolingBufferWriter for optimal single-buffer approach
        using var bufferWriter = new PoolingBufferWriter<byte>(MemoryPool<byte>.Shared.ToAllocator());

        // Write placeholder header first (4 bytes length + 1 byte field number)
        var headerSpan = bufferWriter.GetSpan(5);
        bufferWriter.Advance(5);

        // Serialize directly into the buffer writer (after the header space)
        //Serializer.Serialize(bufferWriter, message);
        MemoryPackSerializer.Serialize(bufferWriter, message);

        // Get the count BEFORE calling DetachBuffer (which resets the state)
        int totalMessageLength = bufferWriter.WrittenCount;
        int serializedLength = totalMessageLength - 5; // Subtract header size
        int protocolLength = 1 + serializedLength; // field number + protobuf data

        // Get writable access to the buffer using DetachBuffer() - this is the key!
        var memoryOwner = bufferWriter.DetachBuffer();
        var writableMemory = memoryOwner.Memory;

        // Update the header directly in the writable buffer - no copying!
        var span = writableMemory.Span;
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(span, protocolLength);
        span[4] = (byte)fieldNumber;

        return memoryOwner;
    }

    public static MemoryOwner<byte> SerializeToMemoryOwner<T>(T message)
    {
        // Use PoolingBufferWriter for optimal single-buffer approach
        using var writer = new PoolingBufferWriter<byte>(MemoryPool<byte>.Shared.ToAllocator());
        writer.GetSpan(4); // Reserve space for length prefix
        writer.Advance(4);

        //Serializer.Serialize(writer, message);
        MemoryPackSerializer.Serialize(writer, message);
        
        var length = writer.WrittenCount - 4;
        var memoryOwner = writer.DetachBuffer();
        var lengthSpan = memoryOwner.Memory.Span;
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(lengthSpan, length);

        return memoryOwner;
    }

}