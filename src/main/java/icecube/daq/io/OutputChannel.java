package icecube.daq.io;

import java.nio.ByteBuffer;

/**
 * Base output channel.
 */
public interface OutputChannel
{
    /**
     * Add data to the output queue.
     *
     * @param buf data to be transmitted.
     */
    void receiveByteBuffer(ByteBuffer buf);
}
