using System.Buffers.Binary;

namespace Common;

public sealed class TimestampBox
{
    public DateTimeOffset Timestamp { get; set; }

    // length(4)
    // type(4)
    // timestamp(8)
    public const int Length = 4 + 4 + 8;

    public byte[] Serialize()
    {
        var buffer = new byte[Length];
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), buffer.Length);
        buffer[4] = (byte)'f';
        buffer[5] = (byte)'r';
        buffer[6] = (byte)'e';
        buffer[7] = (byte)'e';
        // Payload is little endian, because we do not do that big endian funny business if we can avoid it.
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(4 + 4, 8), Timestamp.ToUnixTimeMilliseconds());

        return buffer;
    }

    /// <summary>
    /// Whether we can identify a timestamp box in the given bytes.
    /// </summary>
    public static bool ExistsIn(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != Length)
            return false;

        var boxLength = BinaryPrimitives.ReadInt32BigEndian(bytes.Slice(0, 4));

        if (boxLength != Length)
            return false;

        if (bytes[4] != (byte)'f')
            return false;

        if (bytes[5] != (byte)'r')
            return false;

        if (bytes[6] != (byte)'e')
            return false;

        if (bytes[7] != (byte)'e')
            return false;

        return true;
    }

    public static TimestampBox Deserialize(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != Length)
            throw new ArgumentException("Wrong payload length.", nameof(bytes));

        var unixTimeMilliseconds = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(4 + 4, 8));
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(unixTimeMilliseconds);

        return new TimestampBox
        {
            Timestamp = timestamp
        };
    }
}
