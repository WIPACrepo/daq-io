package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IByteBufferReceiver;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;

/**
 * Output channel.
 */
public interface OutputChannel
    extends IByteBufferReceiver, IOChannel
{
    /**
     * Try to transmit all output.  This may take some time and is subject
     * to the whims of the remote end of the channel.
     */
    void flushOutQueue();

    /**
     * Is there data queued for output?
     *
     * @return <tt>true</tt> if the output queue is not empty
     */
    boolean isOutputQueued();

    /**
     * Add data to the output queue.
     *
     * @param buf data to be transmitted.
     */
    void receiveByteBuffer(ByteBuffer buf);
}
