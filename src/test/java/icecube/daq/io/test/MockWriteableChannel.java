package icecube.daq.io.test;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;

import java.io.IOException;

/**
 * Mock WriteableChannel
 */
class MockWriteableChannel
    implements WritableByteChannel
{
    /**
     * Unimplemented.
     */
    public void close()
        throws IOException
    {
        throw new IOException("Unimplemented");
    }

    /**
     * Unimplemented.
     */
    public boolean isOpen()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Pretend to write something.
     */
    public int write(ByteBuffer buf)
        throws IOException
    {
        return buf.limit();
    }
}
