package icecube.daq.io;

import java.nio.ByteBuffer;

/**
 * Parent engine for an input/output channel.
 */
public interface IOChannelParent
{
    /**
     * Channel encountered an error.
     *
     * @param chan channel
     * @param buf ByteBuffer which caused the error (may ne <tt>null</tt>)
     * @param ex exception (may be null)
     */
    void channelError(IOChannel chan, ByteBuffer buf, Exception ex);

    /**
     * Channel has stopped.
     *
     * @param chan channel
     */
    void channelStopped(IOChannel chan);
}
