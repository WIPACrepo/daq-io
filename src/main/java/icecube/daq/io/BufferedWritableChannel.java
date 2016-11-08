package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Duplicates BufferedOutputStream, but utilizing channels
 * instead of streams.
 *
 * This class is required to support the implementation of
 * a blocking IO engine withing the DAQComponent framework
 * which is channel-oriented.
 */

public class BufferedWritableChannel implements WritableByteChannel
{

    private final IByteBufferCache bufferCache;
    private final WritableByteChannel delegate;
    private final ByteBuffer buffer;
    private int msgsBuffered;
    private long numSent;

    // yuk!
    private final int[] bufferSizes;


    public BufferedWritableChannel(final IByteBufferCache bufferCache,
                                   final WritableByteChannel delegate,
                                   final int size)
    {
        this.bufferCache = bufferCache;
        this.delegate = delegate;
        this.buffer = ByteBuffer.allocate(size);
        this.bufferSizes = new int[size];
    }

    @Override
    public int write(final ByteBuffer src) throws IOException
    {
        int msgSize = src.remaining();

        if(msgSize > buffer.remaining())
        {
            flush();
        }

        // The message is bigger than buffer capacity,
        // just write directly,
        if(msgSize > buffer.remaining())
        {
            sendComplete(src);
            numSent++;
            bufferCache.returnBuffer(msgSize);
        }
        else
        {
            buffer.put(src);

            // track what is buffered on a per-message basis
            bufferSizes[msgsBuffered] = msgSize;
            msgsBuffered++;
        }

        return msgSize;
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        flush();
        delegate.close();
    }

    /**
     * @return The number of bytes buffered.
     */
    public int bufferedBytes()
    {
        return buffer.position();
    }

    /**
     * @return The number of messages buffered.
     */
    public int bufferedMessages()
    {
        return msgsBuffered;
    }

    /**
     * @return The number of messages sent.
     */
    public long numSent()
    {
        return numSent;
    }

    /**
     * Write buffered data out to delegate.
     * @throws IOException
     */
    public void flush() throws IOException
    {
        // NOTE: This is the sole method that manages buffer
        //       draining. buffer.flip() and buffer.clear()
        //       are utilized here, and no where else.
        buffer.flip();
        sendComplete(buffer);

        for (int i = 0; i < msgsBuffered; i++)
        {
            bufferCache.returnBuffer(bufferSizes[i]);
        }
        numSent+=msgsBuffered;
        msgsBuffered=0;
        buffer.clear();
    }

    /**
     * Shoehorn alert. This method is required to precisely match the
     * accounting practices in SimpleOutputEngine which utilizes a
     * stop message that is not accounted in the buffer cache but is
     * counted as a sent message.
     */
    void writeEndMessage(ByteBuffer msg) throws IOException
    {
        flush();
        numSent++;
        sendComplete(msg);
    }

    /**
     * Write a buffer to the delegate, looping to ensure complete transfer
     * in the case of non-blocking io.
     * @param buf
     * @throws IOException
     */
    private void sendComplete(final ByteBuffer buf) throws IOException
    {
        int count = buf.remaining();
        int written = 0;
        while(written < count )
        {
            written+= delegate.write(buf);
        }
    }


}
