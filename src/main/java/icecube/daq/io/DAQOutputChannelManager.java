package icecube.daq.io;

/**
 * Output channel manager.
 */
public interface DAQOutputChannelManager
{
    /**
     * Return the single output channel.
     *
     * @return output channel
     */
    OutputChannel getChannel();
}
