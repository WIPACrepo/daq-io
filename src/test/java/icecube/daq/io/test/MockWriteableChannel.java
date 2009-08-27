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
    private IOException closeException;

    /**
     * Unimplemented.
     */
    public void close()
        throws IOException
    {
        if (closeException != null) {
            throw closeException;
        }
    }

    /**
     * Unimplemented.
     */
    public boolean isOpen()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Set the exception to be thrown when this channel is closed.
     *
     * @param ioe exception to be thrown
     */
    public void setCloseException(IOException ioe)
    {
        closeException = ioe;
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
