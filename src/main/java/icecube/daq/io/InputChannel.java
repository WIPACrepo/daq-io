package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import org.apache.log4j.Logger;

public abstract class InputChannel
    implements IOChannel
{
    public static final long DEFAULT_MAX_BYTES_ALLOCATION_LIMIT = 200000000;

    public static final long PERCENT_STOP_ALLOCATION = 70;
    public static final long PERCENT_RESTART_ALLOCATION = 50;

    /** logging object */
    private static final Logger LOG = Logger.getLogger(InputChannel.class);

    /** size of initial integer payload length */
    private static final int INT_SIZE = 4;

    private IOChannelParent parent;
    private SelectableChannel channel;
    private String name;
    private IByteBufferCache bufMgr;
    private ByteBuffer inputBuf;
    private int bufPos;

    private boolean stopped;

    // buffer manager limits
    private long limitToStopAllocation = 0;
    private long limitToRestartAllocation = 0;
    private boolean allocationStopped = false;

    // input statistics
    private long bytesReceived;
    private long recordsReceived;
    private long stopsReceived;

    private static int nextId = 1;
    final int id = nextId++;

    public InputChannel(IOChannelParent parent, SelectableChannel channel,
                        String name, IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        this.parent = parent;
        this.channel = channel;
        this.name = name + ":" + id;
        this.bufMgr = bufMgr;
        this.inputBuf = ByteBuffer.allocate(bufSize);

        stopped = true;

        if (channel.isBlocking()) {
            channel.configureBlocking(false);
        }

        setAllocationLimits();
    }

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
            LOG.error("******** Reset " + name + " limit to capacity");
            inputBuf.limit(inputBuf.capacity());
        }
    }

    public void close()
        throws IOException
    {
        channel.close();
    }

    private ByteBuffer fillBuffer(int length)
    {
final boolean DEBUG_FILL = false;
if(DEBUG_FILL)System.err.println("FillTop "+inputBuf+" bufPos "+bufPos);
        ByteBuffer payloadBuf = bufMgr.acquireBuffer(length);
        if (payloadBuf == null) {
            LOG.error("Cannot acquire " + name + " " + length +
                      "-byte buffer");
            return null;
        }

        // now fix up actual receive buffer and copy in length
        payloadBuf.position(0);
        payloadBuf.limit(length);

        int pos = inputBuf.position();
        int lim = inputBuf.limit();

        if (lim != inputBuf.capacity()) {
            LOG.error("Surprise!  " + name + " input buffer " + inputBuf +
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

if(DEBUG_FILL)System.err.println("FillEnd "+inputBuf+" bufPos "+bufPos+" payBuf "+payloadBuf);
        return payloadBuf;
    }

    long getBufferCurrentAcquiredBuffers()
    {
        return bufMgr.getCurrentAquiredBuffers();
    }

    long getBufferCurrentAcquiredBytes()
    {
        return bufMgr.getCurrentAquiredBytes();
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

    IOChannelParent getParent()
    {
        return parent;
    }

    long getRecordsReceived()
    {
        return recordsReceived;
    }

    long getStopMessagesReceived()
    {
        return stopsReceived;
    }

    boolean isAllocationStopped()
    {
        return allocationStopped;
    }

    public boolean isOpen()
    {
        return channel.isOpen();
    }

    public boolean isStopped()
    {
        return stopped;
    }

    public void notifyOnStop()
    {
        if (parent != null) {
            parent.channelStopped(this);
        }
    }

    public void processSelect(SelectionKey selKey)
        throws IOException
    {
final boolean DEBUG_SELECT = false;
if(DEBUG_SELECT)System.err.println("SelTop "+inputBuf);
        int numBytes = ((ReadableByteChannel) channel).read(inputBuf);
        if (numBytes < 0) {
            channel.close();
            notifyOnStop();
            throw new ClosedChannelException();
        }

if(DEBUG_SELECT)System.err.println("SelGot "+inputBuf);

        if (stopped) {
if(DEBUG_SELECT)System.err.println("SelStopping");
            // cancel our registration with the Selector
            selKey.cancel();
            // throw away input
            inputBuf.clear();
            // all done
            return;
        }

        while (true) {
if(DEBUG_SELECT)System.err.println("SelLoop");
            // if buffer does not contain enough bytes for the payload length...
            if (inputBuf.position() < bufPos + INT_SIZE) {
if(DEBUG_SELECT)System.err.println("  NotSize "+inputBuf+" bufPos "+bufPos);
                // if buffer cannot hold the payload length...
                if (inputBuf.limit() < bufPos + INT_SIZE) {
                    // adjust/expand buffer to make room for payload length
                    adjustOrExpandInputBuffer(4);
                }

                // wait for more input
                break;
            }

            int length = inputBuf.getInt(bufPos);
if(DEBUG_SELECT)System.err.println("  PayLen "+length);

            // if it's an impossible length...
            if (length < INT_SIZE) {
if(DEBUG_SELECT)System.err.println("  BadLen");
                // this really should not happen
                LOG.error("Huh?  Saw " + length + "-byte payload for " +
                          name);
                if (length <= 0) {
                    length = 4;
                }
                bufPos += length;

                // this is probably a lost cause but try for another payload
                continue;
            }

            // if this is a stop message...
            if (length == INT_SIZE) {
if(DEBUG_SELECT)System.err.println("  GotStop");
                stopped = true;
                stopsReceived++;
                notifyOnStop();
                inputBuf.clear();
                break;
            }

            // check for allocation limits
            if (bufMgr.getCurrentAquiredBytes() >= limitToStopAllocation) {
                if (!allocationStopped) {
                    LOG.error(name + " channel#" + id +
                              " stopped: AcqBytes " +
                              bufMgr.getCurrentAquiredBytes() +
                              " >= limit " + limitToStopAllocation);
if(DEBUG_SELECT)System.err.println("  AllocStopped");
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

if(DEBUG_SELECT)System.err.println("  RestartAlloc");
                // restart allocation
                allocationStopped = false;
                LOG.error(name + " channel#" + id +
                          " restarted: AcqBytes " +
                          bufMgr.getCurrentAquiredBytes() + " <= limit " +
                          limitToRestartAllocation);
            }

            // if buffer does not contain enough bytes for the payload length...
            if (inputBuf.position() < bufPos + length) {
if(DEBUG_SELECT)System.err.println("  SmallBuf");
                // if buffer cannot hold the payload length...
                if (inputBuf.limit() < bufPos + length) {
                    // adjust/expand buffer to make room for payload length
                    adjustOrExpandInputBuffer(length);
                }

                // wait for more input
                break;
            }

            ByteBuffer payBuf = fillBuffer(length);
            if (payBuf == null) {
if(DEBUG_SELECT)System.err.println("  NullBuf");
                break;
            }

            //if (splicerAvailable()) {
            //    // do not execute exitRecvBody because we
            //    // need to wait for permission to log to splicer
            //    enterSplicerWait();
            //    return;
            //}

            payBuf.flip();
if(DEBUG_SELECT)System.err.println("  Got "+payBuf);
            pushPayload(payBuf);
        }
    }

    public abstract void pushPayload(ByteBuffer payBuf)
        throws IOException;

    public void register(Selector sel)
        throws ClosedChannelException
    {
        channel.register(sel, SelectionKey.OP_READ, this);
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

    public void startReading()
    {
        stopped = false;
    }

    @Override
    public String toString()
    {
        return parent.toString() + "=>InputChannel#" + id;
    }
}
