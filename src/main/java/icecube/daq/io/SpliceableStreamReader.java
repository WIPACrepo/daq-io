package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpliceableStreamReader
    extends DAQStreamReader
    implements LimitedChannelParent
{
    private static final Log LOG =
        LogFactory.getLog(SpliceableStreamReader.class);

    // maximum number of stop attempts
    private static final int MAX_STOP_TRIES = 10;

    private Splicer splicer;
    private SpliceableFactory factory;
    /** maximum strand depth */
    private int maxDepth;

    private ArrayList<LimitedChannel> limitedChannels =
        new ArrayList<LimitedChannel>();

    public SpliceableStreamReader(String name, Splicer splicer,
                                  SpliceableFactory factory)
        throws IOException
    {
        this(name, DEFAULT_BUFFER_SIZE, splicer, factory, Integer.MAX_VALUE);
    }

    public SpliceableStreamReader(String name, Splicer splicer,
                                  SpliceableFactory factory, int maxDepth)
        throws IOException
    {
        this(name, DEFAULT_BUFFER_SIZE, splicer, factory, maxDepth);
    }

    public SpliceableStreamReader(String name, int bufferSize,
                                  Splicer splicer, SpliceableFactory factory)
        throws IOException
    {
        this(name, DEFAULT_BUFFER_SIZE, splicer, factory, Integer.MAX_VALUE);
    }

    public SpliceableStreamReader(String name, int bufferSize,
                                  Splicer splicer, SpliceableFactory factory,
                                  int maxDepth)
        throws IOException
    {
        super(name, bufferSize);

        if (splicer == null) {
            throw new IllegalArgumentException("Splicer cannot be null");
        }
        this.splicer = splicer;

        if (factory == null) {
            final String errMsg = "SpliceableFactory cannot be null";
            throw new IllegalArgumentException(errMsg);
        }
        this.factory = factory;

        this.maxDepth = maxDepth;
    }

    @Override
    public InputChannel createChannel(SelectableChannel channel, String name,
                                      IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        return new SpliceableInputChannel(this, channel, name, bufMgr, bufSize,
                                          factory, maxDepth);
    }

    public synchronized Integer[] getStrandDepth()
    {
        // a negative number indicates a null strand end
        Integer nullDepth = Integer.valueOf(-1);

        ArrayList strandDepth = new ArrayList();
        for (InputChannel chan : listChannels()) {
            SpliceableInputChannel sChan = (SpliceableInputChannel) chan;

            Integer depth;
            if (!sChan.hasStrandTail()) {
                depth = nullDepth;
            } else {
                depth = Integer.valueOf(sChan.getStrandTailDepth());
            }
            strandDepth.add(depth);
        }
        return (Integer[]) strandDepth.toArray(new Integer[0]);
    }

    @Override
    public String getStringExtra()
    {
        return ",depth " + getTotalStrandDepth();
    }

    public synchronized int getTotalStrandDepth()
    {
        int totalDepth = 0;

        for (InputChannel chan : listChannels()) {
            SpliceableInputChannel sChan = (SpliceableInputChannel) chan;
            if (sChan.hasStrandTail()) {
                int depth = sChan.getStrandTailDepth();
                if (depth > 0) {
                    totalDepth += depth;
                }
            }
        }

        return totalDepth;
    }

    /**
     * Find and wake limited channels which have dropped below their limit.
     */
    @Override
    public void runSubprocess()
    {
        synchronized (limitedChannels) {
            Iterator<LimitedChannel> iter = limitedChannels.iterator();
            while (iter.hasNext()) {
                LimitedChannel chan = iter.next();
                if (chan.isUnderLimit()) {
                    chan.wakeChannel();
                    iter.remove();
                }
            }
        }
    }

    @Override
    public void startProcessing()
    {
        int tries = 0;
        while (splicer.getState() != Splicer.State.STOPPED) {
            if (++tries > MAX_STOP_TRIES) {
                throw new Error("Couldn't stop splicer");
            }

            if (tries == 1 && LOG.isWarnEnabled()) {
                LOG.warn("Splicer should have been in STOPPED state, not " +
                         splicer.getState().name() +
                         ".  Calling Splicer.forceStop()");
            }

            splicer.forceStop();

            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("problem while sleeping: ", ie);
                }
            }
        }

        try {
            makeReverseConnections();
        } catch (IOException ioe) {
            throw new RuntimeException("Cannot make reverse connections", ioe);
        }

        if (splicer.getStrandCount() != 0) {
            LOG.error("splicer should not have any leftover strands-- count: " +
                      splicer.getStrandCount());
        } else {
            for (InputChannel cd : listChannels()) {
                SpliceableInputChannel sChan = (SpliceableInputChannel) cd;
                sChan.setStrandTail(splicer.beginStrand());
            }
        }

        splicer.start();

        super.startProcessing();
    }

    /**
     * Add the limited channel to a watchlist and put the calling thread in
     * a WAIT state.  The channel will be notified (via
     * <tt>LimitedChannel.wakeChannel()<tt>) when it's no longer limited.
     *
     * @param chan linmited channel
     */
    @Override
    public void watchLimitedChannel(LimitedChannel chan)
    {
        synchronized (limitedChannels) {
            limitedChannels.add(chan);
        }
        synchronized (chan) {
            Thread thrd = Thread.currentThread();

            LOG.error(chan.toString() +
                      " strand tail is too large -- pausing " +
                      thrd.getName());
            try {
                chan.wait();
                LOG.error(chan.toString() + " strand tail has resumed " +
                          thrd.getName());
            } catch (InterruptedException iex) {
                LOG.error(chan.toString() + " interrupted waiting " +
                          thrd.getName(), iex);
            }
        }
    }
}
