package icecube.daq.io;

import java.nio.ByteBuffer;

/**
 * Output channel.
 */
public interface QueuedOutputChannel
    extends IOChannel, OutputChannel
{
    /**
     * Try to transmit all output.  This may take some time and is subject
     * to the whims of the remote end of the channel.
     */
    void flushOutQueue();

    /**
     * Is this channel paused because of a full output queue?
     *
     * @return <tt>true</tt> if the engine is paused
     */
    boolean isOutputPaused();

    /**
     * Is there data queued for output?
     *
     * @return <tt>true</tt> if the output queue is not empty
     */
    boolean isOutputQueued();

    /**
     * Receives a ByteBuffer from a source.
     * @param tBuffer ByteBuffer the new buffer to be processed.
     */
    @Override
    void receiveByteBuffer(ByteBuffer tBuffer);
}
