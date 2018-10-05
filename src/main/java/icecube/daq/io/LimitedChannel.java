package icecube.daq.io;

/**
 * A channel which can be paused if it exceeds an internal limit
 */
public interface LimitedChannel
{
    /**
     * Is this channel over its limit?
     *
     * @return <tt>true</tt> if this channel has exceeded its limit
     */
    boolean isOverLimit();

    /**
     * Has this channel returned to a manageable level after having gone
     * over its limit?
     *
     * @return <tt>true</tt> if channel is under its limit
     */
    boolean isUnderLimit();

    /**
     * Used by LimitedChannelParent to wake a channel which is no longer
     * limited.
     */
    void wakeChannel();
}
