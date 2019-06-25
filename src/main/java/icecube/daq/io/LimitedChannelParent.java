package icecube.daq.io;

public interface LimitedChannelParent
    extends IOChannelParent
{
    /**
     * Return parent's name
     *
     * @return name
     */
    String getName();

    /**
     * Add the limited channel to a watchlist and put the calling thread in
     * a WAIT state.  The channel should be notified (via
     * <tt>LimitedChannel.wakeChannel()<tt>) when it's no longer limited.
     *
     * @param chan linmited channel
     */
    void watchLimitedChannel(LimitedChannel chan);
}
