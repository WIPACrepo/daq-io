package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;

import java.io.IOException;

import java.nio.channels.SelectableChannel;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpliceablePayloadReader
    extends PayloadReader
{
    private static final Log LOG =
        LogFactory.getLog(SpliceablePayloadReader.class);

    // default maximum size of strand queue
    private static final int DEFAULT_STRAND_QUEUE_MAX = 40000;

    private Splicer splicer;
    private SpliceableFactory factory;

    public SpliceablePayloadReader(String name, Splicer splicer,
                                   SpliceableFactory factory)
        throws IOException
    {
        this(name, DEFAULT_BUFFER_SIZE, splicer, factory);
    }

    public SpliceablePayloadReader(String name, int bufferSize,
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

    public InputChannel createChannel(SelectableChannel channel,
                                      IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        return new SpliceableInputChannel(this, channel, bufMgr, bufSize,
                                          factory);
    }

    public synchronized Integer[] getStrandDepth()
    {
        // a negative number indicates a null strand end
        Integer nullDepth = new Integer(-1);

        ArrayList strandDepth = new ArrayList();
        for (InputChannel chan : listChannels()) {
            SpliceableInputChannel sChan = (SpliceableInputChannel) chan;

            Integer depth;
            if (!sChan.hasStrandTail()) {
                depth = nullDepth;
            } else {
                depth = new Integer(sChan.getStrandTailDepth());
            }
            strandDepth.add(depth);
        }
        return (Integer[]) strandDepth.toArray(new Integer[0]);
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

    public void startProcessing()
    {
        while (splicer.getState() != Splicer.STOPPED) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Splicer should have been in STOPPED state, not " +
                         splicer.getStateString() +
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
}
