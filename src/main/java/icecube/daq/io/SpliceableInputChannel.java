package icecube.daq.io;

import icecube.daq.splicer.ClosedStrandException;
import icecube.daq.splicer.OrderingException;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.StrandTail;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IPayload;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.SelectableChannel;

import java.util.ArrayList;

import java.util.zip.DataFormatException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SpliceableInputChannel
    extends InputChannel
    implements Runnable
{
    /** logging object */
    private static final Log LOG =
        LogFactory.getLog(SpliceableInputChannel.class);

    private static final int DEFAULT_STRAND_QUEUE_MAX = 40000;

    private SpliceableFactory factory;
    private StrandTail strandTail;
    private Thread thread;
    private ArrayList<Spliceable> queue;

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
        this.queue = new ArrayList<Spliceable>();
    }

    int getQueueDepth()
    {
        return queue.size();
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

    public boolean isRunning()
    {
        return thread != null;
    }

    public void notifyOnStop()
    {
        // since this is a SpliceablePayloadReceiveChannel, we
        // will have to shut down the splicer if necessary
        if (LOG.isInfoEnabled()) {
            LOG.info("pushing LAST_POSSIBLE_SPLICEABLE");
        }

        pushSpliceable(Splicer.LAST_POSSIBLE_SPLICEABLE);

        thread = null;

        super.notifyOnStop();
    }

    public void pushPayload(ByteBuffer payBuf)
    {
        Spliceable spliceable = factory.createSpliceable(payBuf);
        if (spliceable == null) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Couldn't use buffer (limit " +
                          payBuf.limit() + ", capacity " + payBuf.capacity() +
                          ") to create payload (length " +
                          (payBuf.limit() < 4 ? -1 : payBuf.getInt(0)) +
                          ", capacity " +
                          (payBuf.limit() < 8 ? -1 : payBuf.getInt(4)) + ")");
            }

            throw new RuntimeException("Couldn't create a Spliceable");
        }

        pushSpliceable(spliceable);
    }

    private void pushSpliceable(Spliceable spliceable)
    {
        if (thread == null) {
            LOG.error("Pushed spliceable without active thread!");
        }

        synchronized (queue) {
            queue.add(spliceable);
            queue.notifyAll();
        }
    }

    public void run()
    {
        ArrayList<Spliceable> workList = new ArrayList<Spliceable>();

        while (isRunning() || queue.size() > 0) {
            synchronized (queue) {
                while (isRunning() && queue.size() == 0) {
                    try {
                        queue.wait(100);
                    } catch (InterruptedException ie) {
                        // ignore interrupts
                    }
                }

                workList.addAll(queue);
                queue.clear();
            }

            for (Spliceable spliceable : workList) {
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
                    if (spliceable instanceof ILoadablePayload) {
                        ILoadablePayload payload =
                            (ILoadablePayload) spliceable;

                        if (LOG.isErrorEnabled()) {
                            if (LOG.isErrorEnabled()) {
                                LOG.error("Couldn't push payload type " +
                                          payload.getPayloadType() +
                                          ", length " +
                                          payload.getPayloadLength() +
                                          ", time " +
                                          payload.getPayloadTimeUTC() +
                                          "; recycling", ex);
                            }
                        }

                        payload.recycle();
                    } else if (LOG.isErrorEnabled()) {
                        LOG.error("Couldn't push " +
                                  spliceable.getClass().getName(), ex);
                    }
                }
            }

            workList.clear();
        }

        if (!strandTail.isClosed()) {
            strandTail.close();
        }
    }

    public void setStrandTail(StrandTail strandTail)
    {
        if (strandTail == null) {
            throw new IllegalArgumentException("StrandTail cannot be null");
        }

        this.strandTail = strandTail;
    }

    public void startReading()
    {
        if (strandTail == null) {
            // just to be paranoid, check that strandTail has
            // been initialized before continuing.
            throw new Error("Strand tail has not been initialized");
        }

        if (thread != null) {
            LOG.error("Thread is already running!");
        } else {
            thread = new Thread(this);
            thread.setName("QueueThread");
            thread.start();
        }

        super.startReading();
    }

    public String toString()
    {
        return "SpliceableInputChannel#" + id;
    }
}
