using System;
using Minerva.DB_Server.Storage;

namespace Minerva.DB_Server;

public static class ByteArrayOperators
{
    public static byte[] Concat(byte[] first, byte[] second)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);

        byte[] result = new byte[first.Length + second.Length];
        Buffer.BlockCopy(first, 0, result, 0, first.Length);
        Buffer.BlockCopy(second, 0, result, first.Length, second.Length);
        return result;
    }
}