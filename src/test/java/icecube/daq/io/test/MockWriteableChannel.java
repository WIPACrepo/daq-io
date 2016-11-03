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
    private IOException writeException;

    private long bytesWritten;

    public long getBytesWritten()
    {
        return bytesWritten;
    }

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
     * Set the exception to be thrown when this channel is written to.
     *
     * @param ioe exception to be thrown
     */
    public void setWriteException(IOException ioe)
    {
        writeException = ioe;
    }

    /**
     * Pretend to write something.
     */
    public int write(ByteBuffer buf)
        throws IOException
    {
        if(writeException != null)
        {
            throw writeException;
        }
        int remaining = buf.remaining();
        bytesWritten += remaining;

        return remaining;
    }
}
