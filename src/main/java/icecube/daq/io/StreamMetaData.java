package icecube.daq.io;

/**
 * Simple class which binds together payload counts and last payload time.
 */
public class StreamMetaData
{
    /** Number of payloads */
    private long count;
    /** Most recent payload time */
    private long ticks;

    /**
     * Tie together count and tick values
     *
     * @param count number of payloads
     * @param ticks time of most recent payload
     */
    public StreamMetaData(long count, long ticks)
    {
        this.count = count;
        this.ticks = ticks;
    }

    /**
     * Return number of payloads
     *
     * @return number of payloads
     */
    public long getCount()
    {
        return count;
    }

    /**
     * Return last payload time
     *
     * @return last payload time
     */
    public long getTicks()
    {
        return ticks;
    }
}
