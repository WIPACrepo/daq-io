package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class SimpleChannel
    implements IOChannel, Runnable
{
    /** Maximum number of allocatable bytes */
    public static final long DEFAULT_MAX_BYTES_ALLOCATION_LIMIT = 200000000;

    /** Percent of max bytes at which allocation is stopped. */
    public static final long PERCENT_STOP_ALLOCATION = 70;
    /** Percent of max bytes at which allocation is resumed. */
    public static final long PERCENT_RESTART_ALLOCATION = 50;

    /** logging object */
    private static final Log LOG = LogFactory.getLog(SimpleChannel.class);

    /** size of initial integer payload length */
    private static final int INT_SIZE = 4;

    private IOChannelParent parent;
    private String name;
    private SelectableChannel channel;
    private IByteBufferCache bufMgr;

    private Thread thread;

    private ByteBuffer inputBuf;
    private int bufPos;
    private boolean isDisposing;

    // buffer manager limits
    private long limitToStopAllocation;
    private long limitToRestartAllocation;
    private boolean allocationStopped;

    // input statistics
    private long bytesReceived;
    private long recordsReceived;
    private long stopsReceived;

    public SimpleChannel(IOChannelParent parent, String name,
                         SelectableChannel channel, IByteBufferCache bufMgr,
                         int bufSize)
    {
        this.parent = parent;
        this.name = name;
        this.channel = channel;
        this.bufMgr = bufMgr;

        inputBuf = ByteBuffer.allocate(bufSize);

        setAllocationLimits();
    }

    /**
     * Adjust or expand the input buffer to fit the current payload.
     *
     * @param length current payload length
     */
    private void adjustOrExpandInputBuffer(int length)
    {
        inputBuf.limit(inputBuf.position());
        inputBuf.position(bufPos);
        bufPos = 0;

        if (length <= inputBuf.capacity()) {
            inputBuf.compact();
        } else {
            ByteBuffer newBuf = ByteBuffer.allocate(inputBuf.capacity() * 2);
            newBuf.put(inputBuf);
            inputBuf = newBuf;
        }

        if (inputBuf.limit() != inputBuf.capacity()) {
            LOG.error("******** Reset limit to capacity");
            inputBuf.limit(inputBuf.capacity());
        }
    }

    /**
     * Destroy this channel.
     */
    void destroy()
    {
        stopProcessing();

        try {
            channel.close();
        } catch (IOException ioe) {
            parent.channelError(this, null, ioe);
        }
    }

    /**
     * Return a newly allocated buffer which is filled with the next payload
     * from the input buffer.
     *
     * @param length length of next payload
     *
     * @return payload buffer
     */
    private ByteBuffer fillBuffer(int length)
    {
        ByteBuffer payloadBuf = bufMgr.acquireBuffer(length);
        if (payloadBuf == null) {
            LOG.error("Cannot acquire " + length + "-byte buffer");
            return null;
        }

        // now fix up actual receive buffer and copy in length
        payloadBuf.position(0);
        payloadBuf.limit(length);

        int pos = inputBuf.position();
        int lim = inputBuf.limit();

        if (lim != inputBuf.capacity()) {
            LOG.error("Surprise!  Input buffer " + inputBuf +
                      " capacity != limit");
        }

        inputBuf.position(bufPos);
        inputBuf.limit(bufPos + length);

        payloadBuf.put(inputBuf);

        inputBuf.limit(lim);

        if (bufPos + length == pos) {
            inputBuf.position(0);
            bufPos = 0;
        } else {
            inputBuf.position(pos);
            bufPos += length;
        }

        bytesReceived += length;
        recordsReceived++;

        return payloadBuf;
    }

    long getBytesReceived()
    {
        return bytesReceived;
    }

    long getLimitToStopAllocation()
    {
        return limitToStopAllocation;
    }

    long getLimitToRestartAllocation()
    {
        return limitToRestartAllocation;
    }

    long getRecordsReceived()
    {
        return recordsReceived;
    }

    long getStopMessagesReceived()
    {
        return stopsReceived;
    }

    private void handleBuffer()
    {
        while (true) {
            // if buffer does not contain enough bytes for the payload length...
            if (inputBuf.position() < bufPos + INT_SIZE) {
                // if buffer cannot hold the payload length...
                if (inputBuf.limit() < bufPos + INT_SIZE) {
                    // adjust/expand buffer to make room for payload length
                    adjustOrExpandInputBuffer(4);
                }

                // wait for more input
                break;
            }

            int length = inputBuf.getInt(bufPos);

            // if it's an impossible length...
            if (length < INT_SIZE) {
                // this really should not happen
                LOG.error("Huh?  Saw " + length + "-byte payload!?!?");
                if (length == 0) {
                    length = 4;
                }
                bufPos += length;

                // this is probably a lost cause but try for another payload
                continue;
            }

            // if this is a stop message...
            if (length == INT_SIZE) {
/*
                stopped = true;
*/
                stopsReceived++;
                notifyOnStop();
                inputBuf.clear();
                break;
            }

            // check for allocation limits
            if (bufMgr.getCurrentAquiredBytes() >= limitToStopAllocation) {
                if (!allocationStopped) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Channel " + name + " stopped: AcqBytes " +
                                  bufMgr.getCurrentAquiredBytes() +
                                  " >= limit " + limitToStopAllocation);
                    }
                    allocationStopped = true;
                }

                break;
            }

            // if byte buffer allocation was stopped...
            if (allocationStopped) {
                if (bufMgr.getCurrentAquiredBytes() >
                    limitToRestartAllocation)
                {
                    // give buffer cache a chance to clear out
                    try {
                        Thread.sleep(1000);
                    } catch (Exception ie) {
                        // ignore interrupts
                    }
                    break;
                }

                // restart allocation
                allocationStopped = false;
                if (LOG.isErrorEnabled()) {
                    LOG.error("Channel " + name + " restarted: AcqBytes " +
                              bufMgr.getCurrentAquiredBytes() + " <= limit " +
                              limitToRestartAllocation);
                }
            }

            // if buffer does not contain enough bytes for the payload length...
            if (inputBuf.position() < bufPos + length) {
                // if buffer cannot hold the payload length...
                if (inputBuf.limit() < bufPos + length) {
                    // adjust/expand buffer to make room for payload length
                    adjustOrExpandInputBuffer(length);
                }

                // wait for more input
                break;
            }

            if (isDisposing) {
                if (inputBuf.position() == bufPos + length) {
                    inputBuf.position(0);
                    bufPos = 0;
                } else {
                    bufPos += length;
                }

                continue;
            }

            ByteBuffer payBuf = fillBuffer(length);
            if (payBuf == null) {
                break;
            }

            payBuf.flip();
            try {
                pushPayload(payBuf);
            } catch (IOException ioe) {
                parent.channelError(this, payBuf, ioe);
            }
        }
    }

    /**
     * Is this channel waiting for space to be freed in the buffer cache?
     *
     * @return <tt>true</tt> if we're waiting for space
     */
    boolean isAllocationStopped()
    {
        return allocationStopped;
    }

    /**
     * Is the output thread running?
     *
     * @return <tt>true</tt> if the output thread is alive
     */
    boolean isRunning()
    {
        return thread != null;
    }

    void notifyOnStop()
    {
        if (parent != null) {
            parent.channelStopped(this);
        }
    }

    public abstract void pushPayload(ByteBuffer payBuf)
        throws IOException;

    /**
     * Unimplemented.
     *
     * @param compObserver component observer
     * @param notificationID ID string
     */
    public void registerComponentObserver(DAQComponentObserver compObserver,
                                          String notificationID)
    {
        throw new Error("Unimplemented");
    }

    public void run()
    {
        Selector selector;

        try {
            selector = Selector.open();
            SelectableChannel selChan = (SelectableChannel) channel;
            if ((selChan.validOps() & SelectionKey.OP_READ) == 0) {
                throw new IOException("Channel does not have OP_READ selector");
            }
            if (selChan.isBlocking()) {
                selChan.configureBlocking(false);
            }
            selChan.register(selector, SelectionKey.OP_READ);
        } catch (IOException ioe) {
            parent.channelError(this, null, ioe);
            return;
        }

        while (isRunning()) {
            try {
                selector.select(1000);
            } catch (IOException ioe) {
                parent.channelError(this, null, ioe);
                continue;
            }

            for (SelectionKey key : selector.selectedKeys()) {
                if (!key.isValid() || !key.isReadable()) {
                    LOG.error("Unrecognized SelectionKey #" +
                              key.interestOps() + " (" +
                              (key.isValid() ? "" : "!") + "valid " +
                              (key.isAcceptable() ? "" : "!") + "accept " +
                              (key.isConnectable() ? "" : "!") + "conn " +
                              (key.isReadable() ? "" : "!") + "read " +
                              (key.isWritable() ? "" : "!") + "write " +
                              ")");
                    selector.selectedKeys().clear();
                    continue;
                }

                int numBytes;
                try {
                    numBytes = ((ReadableByteChannel) channel).read(inputBuf);
                } catch (IOException ioe) {
                    parent.channelError(this, null, ioe);
                    LOG.error("Couldn't read from " + name, ioe);
                    numBytes = -1;
                }

                if (numBytes < 0) {
                    try {
                        channel.close();
                        notifyOnStop();
                        //throw new ClosedChannelException();
                    } catch (IOException ioe) {
                        parent.channelError(this, null, ioe);
                    }
                }

                handleBuffer();
            }
        }
    }

    private void setAllocationLimits()
    {
        allocationStopped = false;

        final long maxAllocation;
        if (bufMgr.getIsCacheBounded()) {
            maxAllocation = bufMgr.getMaxAquiredBytes();
        } else {
            maxAllocation = DEFAULT_MAX_BYTES_ALLOCATION_LIMIT;
        }

        limitToStopAllocation =
            ((maxAllocation / 100L) * PERCENT_STOP_ALLOCATION) +
            (((maxAllocation % 100L) * PERCENT_STOP_ALLOCATION) / 100L);
        limitToRestartAllocation =
            ((maxAllocation / 100L) * PERCENT_RESTART_ALLOCATION) +
            (((maxAllocation % 100L) * PERCENT_RESTART_ALLOCATION) / 100L);
    }

    public void startDisposing()
    {
        if (thread == null) {
            throw new Error("Thread is not running");
        }

        isDisposing = true;
    }

    public void startProcessing()
    {
        if (thread != null) {
            throw new Error("Thread is already running");
        }

        thread = new Thread(this);
        thread.setName(name);
        thread.start();
    }

    /**
     * Stop the output thread.
     */
    void stopProcessing()
    {
        thread = null;

        try {
            channel.close();
        } catch (IOException ioe) {
            parent.channelError(this, null, ioe);
        }

        notifyOnStop();
    }

    /**
     * String representation of output channel.
     *
     * @return debugging string
     */
    public String toString()
    {
        return name;
    }
}
