package icecube.daq.io;

import icecube.daq.payload.IByteBufferReceiver;

import java.nio.ByteBuffer;

/**
 * Output channel.
 */
public interface QueuedOutputChannel
    extends IByteBufferReceiver, IOChannel, OutputChannel
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
}
