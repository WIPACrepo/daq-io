package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.splicer.ClosedStrandException;
import icecube.daq.splicer.OrderingException;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.StrandTail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;

import org.apache.log4j.Logger;

public class SpliceableSimpleChannel
    extends SimpleChannel
{
    /** logging object */
    private static final Logger LOG =
        Logger.getLogger(SpliceableSimpleChannel.class);

    private SpliceableFactory factory;
    private StrandTail strandTail;

    SpliceableSimpleChannel(IOChannelParent parent, String name,
                            SelectableChannel channel, IByteBufferCache bufMgr,
                            int bufSize, SpliceableFactory factory)
        throws IOException
    {
        super(parent, name, channel, bufMgr, bufSize);

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

    @Override
    public void notifyOnStop()
    {
        // since this is a SpliceablePayloadReceiveChannel, we
        // will have to shut down the splicer if necessary
        if (LOG.isInfoEnabled()) {
            LOG.info("pushing LAST_POSSIBLE_SPLICEABLE");
        }

        pushSpliceable(SpliceableFactory.LAST_POSSIBLE_SPLICEABLE);

        super.notifyOnStop();
    }

    @Override
    public void pushPayload(ByteBuffer payBuf)
    {
        Spliceable spliceable = factory.createSpliceable(payBuf);
        if (spliceable == null) {
            LOG.error("Couldn't use buffer (limit " +
                      payBuf.limit() + ", capacity " + payBuf.capacity() +
                      ") to create payload (length " +
                      (payBuf.limit() < 4 ? -1 : payBuf.getInt(0)) +
                      ", capacity " +
                      (payBuf.limit() < 8 ? -1 : payBuf.getInt(4)) + ")");

            throw new RuntimeException("Couldn't create a Spliceable");
        }

        pushSpliceable(spliceable);
    }

    private void pushSpliceable(Spliceable spliceable)
    {
        Exception ex;
        try {
            strandTail.push(spliceable);
            ex = null;
        } catch (OrderingException oe) {
            ex = oe;
        } catch (ClosedStrandException cse) {
            ex = cse;
        }

        if (ex != null) {
            if (spliceable instanceof IPayload) {
                IPayload payload = (IPayload) spliceable;

                LOG.error("Couldn't push payload type " +
                          payload.getPayloadType() +
                          ", length " + payload.length() +
                          ", time " + payload.getPayloadTimeUTC() +
                          "; recycling", ex);

                payload.recycle();
            } else {
                LOG.error("Couldn't push " +
                          spliceable.getClass().getName(), ex);
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

    @Override
    public void startProcessing()
    {
        if (strandTail == null) {
            // just to be paranoid, check that strandTail has
            // been initialized before continuing.
            throw new Error("Strand tail has not been initialized");
        }

        super.startProcessing();
    }
}
