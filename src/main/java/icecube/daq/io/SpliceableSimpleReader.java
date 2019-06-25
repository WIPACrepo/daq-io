package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpliceableSimpleReader
    extends SimpleStreamReader
{
    private static final Log LOG =
        LogFactory.getLog(SpliceableSimpleReader.class);

    // maximum number of stop attempts
    private static final int MAX_STOP_TRIES = 10;

    private Splicer splicer;
    private SpliceableFactory factory;

    public SpliceableSimpleReader(String name, Splicer splicer,
                               SpliceableFactory factory)
        throws IOException
    {
        this(name, DEFAULT_BUFFER_SIZE, splicer, factory);
    }

    public SpliceableSimpleReader(String name, int bufferSize,
                               Splicer splicer, SpliceableFactory factory)
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
    }

    @Override
    public SimpleChannel createChannel(String name, SelectableChannel channel,
                                       IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        return new SpliceableSimpleChannel(this, name, channel, bufMgr,
                                           bufSize, factory);
    }

    public synchronized Integer[] getStrandDepth()
    {
        // a negative number indicates a null strand end
        Integer nullDepth = Integer.valueOf(-1);

        ArrayList strandDepth = new ArrayList();
        for (SimpleChannel chan : listChannels()) {
            SpliceableSimpleChannel sChan = (SpliceableSimpleChannel) chan;

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

    public synchronized int getTotalStrandDepth()
    {
        int totalDepth = 0;

        for (SimpleChannel chan : listChannels()) {
            SpliceableSimpleChannel sChan = (SpliceableSimpleChannel) chan;
            if (sChan.hasStrandTail()) {
                int depth = sChan.getStrandTailDepth();
                if (depth > 0) {
                    totalDepth += depth;
                }
            }
        }

        return totalDepth;
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
            for (SimpleChannel cd : listChannels()) {
                SpliceableSimpleChannel sChan = (SpliceableSimpleChannel) cd;
                sChan.setStrandTail(splicer.beginStrand());
            }
        }

        splicer.start();

        super.startProcessing();
    }
}
