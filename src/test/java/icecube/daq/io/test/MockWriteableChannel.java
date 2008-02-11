package icecube.daq.io.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Mock WriteableChannel
 */
public class MockWriteableChannel
    implements WritableByteChannel
{
    /**
     * Unimplemented.
     */
    public void close()
        throws IOException
    {
        throw new Error("Unimplemented");
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
