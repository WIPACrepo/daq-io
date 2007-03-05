package icecube.daq.io;

/**
 * Rudimentary profiler for payload request fulfillment engine.
 */
public class EngineTimer
    extends CodeTimer
{
    /** Amount of time spent getting the next request. */
    public static final int CHECK    = 0;
    public static final int TOPLOOP  = 1;
    public static final int SELECT   = 2;
    public static final int HANDLE   = 3;
    public static final int DOSTATES = 4;
    public static final int NUM_TIMES = 5;

    private static final String[] TIME_NAMES = new String[] {
        "check",
	"toploop",
	"select",
	"handle",
	"dostates",
    };

    /**
     * Create rudimentary profiler.
     */
    public EngineTimer()
    {
        super(NUM_TIMES);
    }

    /**
     * Get current timing info.
     *
     * @return timing info
     */
    public String getStats()
    {
        if (TIME_NAMES.length != NUM_TIMES) {
            throw new Error("Expected " + NUM_TIMES + " titles, not " +
                            TIME_NAMES.length);
        }

        return getStats(TIME_NAMES);
    }

    /**
     * Get current timing info.
     *
     * @return timing info
     */
    public String toString()
    {
        return getStats();
    }
}
