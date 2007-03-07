package icecube.daq.io;

/**
 * Rudimentary profiling timer.
 */
public class CodeTimer
{
    private long startTime;
    private long[] timeAccum;
    private long[] numAccum;
    private long totalTime;

    /**
     * Create a code timer.
     *
     * @param numTimes number of timing points
     */
    public CodeTimer(int numTimes)
    {
        startTime = 0;
        timeAccum = new long[numTimes];
        numAccum = new long[numTimes];
        totalTime = 0;
    }

    /**
     * Assign time to specified timing point.
     *
     * @param num index of timing point
     * @param time amount of time to be added
     *
     * @return time accumulated
     */
    public final long addTime(int num, long time)
    {
        if (num < 0 || num >= timeAccum.length) {
            throw new Error("Illegal timer #" + num);
        }

        timeAccum[num] += time;
        numAccum[num]++;

        totalTime = 0;

        return time;
    }

    /**
     * Get description of current timing statistics.
     *
     * @param title names of timing points
     *
     * @return description of current timing statistics
     */
    public String getStats(String[] title)
    {
        if (title.length != timeAccum.length) {
            throw new Error("Expected " + timeAccum.length + " titles, got " +
                            title.length);
        }

        if (totalTime == 0) {
            for (int i = 0; i < timeAccum.length; i++) {
                totalTime += timeAccum[i];
            }
        }

        StringBuffer buf = new StringBuffer();

        boolean needSpace = false;
        for (int i = 0; i < title.length; i++) {
            if (numAccum[i] > 0) {
                if (!needSpace) {
                    needSpace = true;
                } else {
                    buf.append(' ');
                }

                buf.append(getStats(title[i], timeAccum[i], numAccum[i],
                                    totalTime));
            }
        }

        return buf.toString();
    }

    /**
     * Get description of a single timing point.
     *
     * @param title name of timing point
     * @param num index of timing point
     *
     * @return description of timing point
     */
    public String getStats(String title, int num)
    {
        if (num < 0 || num >= timeAccum.length) {
            throw new Error("Illegal timer #" + num);
        }

        if (totalTime == 0) {
            for (int i = 0; i < timeAccum.length; i++) {
                totalTime += timeAccum[i];
            }
        }

        return getStats(title, timeAccum[num], numAccum[num], totalTime);
    }

    /**
     * Get description of a single timing point.
     *
     * @param title name of timing point
     * @param time accumulated time
     * @param num index of timing point
     * @param totalTime total time used to calculate percent value
     *
     * @return description of timing point
     */
    public String getStats(String title, long time, long num, long totalTime)
    {
        double pct;
        if (totalTime == 0) {
            pct = 0.0;
        } else {
            pct = ((double) time / (double) totalTime) * 100.0;
        }

        String pctStr = Double.toString(pct + 0.005);
        int endPt = pctStr.indexOf('.') + 3;
        if (endPt > 2 && pctStr.length() > endPt) {
            pctStr = pctStr.substring(0, endPt);
        }

        long avgTime;
        if (num == 0) {
            avgTime = 0;
        } else {
            avgTime = time / num;
        }

        return title + ": " + time + "/" + num + "=" + avgTime + "#" +
            pctStr + "%";
    }

    /**
     * Start current timing slice.
     */
    public final void start()
    {
        startTime = System.currentTimeMillis();
    }

    /**
     * Stop current timing and assign accumulated time
     * to specified timing point.
     *
     * @param num index of timing point
     *
     * @return time accumulated
     */
    public final long stop(int num)
    {
        if (num < 0 || num >= timeAccum.length) {
            throw new Error("Illegal timer #" + num);
        } else if (startTime == 0) {
            throw new Error("No timer running");
        }

        final long time = System.currentTimeMillis() - startTime;
        startTime = 0;

        timeAccum[num] += time;
        numAccum[num]++;

        totalTime = 0;

        return time;
    }
}
