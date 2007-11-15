package icecube.daq.io;

import java.nio.ByteBuffer;

/**
 * Parent engine for an output channel.
 */
public interface IChannelParent
{
    /**
     * Channel encountered an error.
     *
     * @param chan channel
     * @param buf ByteBuffer which caused the error (may ne <tt>null</tt>)
     * @ex exception (may be null)
     */
    void channelError(OutputChannel chan, ByteBuffer buf,
                          Exception ex);

    /**
     * Channel has stopped.
     *
     * @param chan channel
     */
    void channelStopped(OutputChannel chan);
}
