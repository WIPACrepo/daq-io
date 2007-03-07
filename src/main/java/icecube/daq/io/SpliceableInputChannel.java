package icecube.daq.io;

import icecube.daq.splicer.ClosedStrandException;
import icecube.daq.splicer.OrderingException;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.StrandTail;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.SelectableChannel;

import java.util.zip.DataFormatException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpliceableInputChannel
    extends InputChannel
{
    /** logging object */
    private static final Log LOG =
        LogFactory.getLog(SpliceableInputChannel.class);

    private static final int DEFAULT_STRAND_QUEUE_MAX = 40000;

    private SpliceableFactory factory;
    private StrandTail strandTail;

    SpliceableInputChannel(InputChannelParent parent, SelectableChannel channel,
                           IByteBufferCache bufMgr, int bufSize,
                           SpliceableFactory factory)
        throws IOException
    {
        super(parent, channel, bufMgr, bufSize);

        if (factory == null) {
            final String errMsg = "SpliceableFactory cannot be null";
            throw new IllegalArgumentException(errMsg);
        }
        this.factory = factory;
    }

    int getStrandTailDepth()
    {
        if (strandTail == null) {
            return 0;
        }

        return strandTail.size();
    }

    boolean hasStrandTail()
    {
        return strandTail != null;
    }

    void notifyOnStop()
    {
        // since this is a SpliceablePayloadReceiveChannel, we
        // will have to shut down the splicer if necessary
        if (LOG.isInfoEnabled()) {
            LOG.info("pushing LAST_POSSIBLE_SPLICEABLE");
        }

        pushSpliceable(Splicer.LAST_POSSIBLE_SPLICEABLE);
        if (!strandTail.isClosed()) {
            strandTail.close();
        }

        super.notifyOnStop();
    }

    public void pushPayload(ByteBuffer payBuf)
    {
        Spliceable spliceable = factory.createSpliceable(payBuf);
        if (spliceable == null) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Couldn't generate a payload from a buf: ");
                LOG.error("buf record: " + payBuf.getInt(0));
                LOG.error("buf limit: " + payBuf.limit());
                LOG.error("buf capacity: " + payBuf.capacity());
            }

            throw new RuntimeException("Couldn't create a Spliceable");
        }

        pushSpliceable(spliceable);
    }

    private void pushSpliceable(Spliceable spliceable)
    {
        try {
            strandTail.push(spliceable);
        } catch (OrderingException oe) {
            // TODO: Need to be reviewed.
            if (LOG.isErrorEnabled()) {
                LOG.error("coudn't push a spliceable object: ", oe);
            }
        } catch (ClosedStrandException cse) {
            // TODO: Need to be reviewed.
            if (LOG.isErrorEnabled()) {
                LOG.error("coudn't push a spliceable object: ", cse);
            }
        }
    }

    public void setStrandTail(StrandTail strandTail)
    {
        if (strandTail == null) {
            throw new IllegalArgumentException("StrandTail cannot be null");
        }

        this.strandTail = strandTail;
    }

    void startReading()
    {
        if (strandTail == null) {
            // just to be paranoid, check that strandTail has
            // been initialized before continuing.
            throw new Error("Strand tail has not been initialized");
        }

        super.startReading();
    }

    public String toString()
    {
        return "SpliceableInputChannel#" + id;
    }
}
