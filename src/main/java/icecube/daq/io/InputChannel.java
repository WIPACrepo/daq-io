package icecube.daq.io;

import icecube.daq.payload.ByteBufferCache;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class InputChannel
{
    /** logging object */
    private static final Log LOG = LogFactory.getLog(PayloadReader.class);

    /** size of initial integer payload length */
    private static final int INT_SIZE = 4;

    private static final long DEFAULT_PERCENT_STOP_ALLOCATION = 70;
    private static final long DEFAULT_PERCENT_RESTART_ALLOCATION = 50;
    private static final long DEFAULT_MAX_BYTES_ALLOCATION_LIMIT = 200000000;

    private InputChannelParent parent;
    private SelectableChannel channel;
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

    private long percentOfMaxStopAllocation =
        DEFAULT_PERCENT_STOP_ALLOCATION;
    private long percentOfMaxRestartAllocation =
        DEFAULT_PERCENT_RESTART_ALLOCATION;

    private static int nextId = 1;
    final int id = nextId++;

    public InputChannel(InputChannelParent parent, SelectableChannel channel,
                        IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        this.parent = parent;
        this.channel = channel;
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
            LOG.error("******** Reset limit to capacity");
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

    long getPercentOfMaxStopAllocation()
    {
        return percentOfMaxStopAllocation;
    }

    long getPercentOfMaxRestartAllocation()
    {
        return percentOfMaxRestartAllocation;
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

    boolean isStopped()
    {
        return stopped;
    }

    void notifyOnStop()
    {
        if (parent != null) {
            parent.channelStopped();
        }
    }

    void processSelect(SelectionKey selKey)
        throws IOException
    {
final boolean DEBUG_SELECT = false;
if(DEBUG_SELECT)System.err.println("SelTop "+inputBuf);
        int numBytes = ((ReadableByteChannel) channel).read(inputBuf);
        if (numBytes < 0) {
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
if(DEBUG_SELECT)System.err.println("  GotStop");
                stopped = true;
                stopsReceived++;
                notifyOnStop();
                inputBuf.clear();
                break;
            }

            // if we can't allocate byte buffers, give up
            if (allocationStopped) {
if(DEBUG_SELECT)System.err.println("  NoAlloc");
                break;
            }

            // check for allocation limits
            if (bufMgr.getCurrentAquiredBytes() >= limitToStopAllocation) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Channel#" + id + " stopped: AcqBytes " +
                              bufMgr.getCurrentAquiredBytes() + " >= limit " +
                              limitToStopAllocation);
                }
                allocationStopped = true;
if(DEBUG_SELECT)System.err.println("  AllocStopped");

                break;
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

    void register(Selector sel)
        throws ClosedChannelException
    {
        channel.register(sel, SelectionKey.OP_READ, this);
    }

    private void setAllocationLimits()
    {
        allocationStopped = false;
        if (bufMgr instanceof ByteBufferCache && ((ByteBufferCache) bufMgr).getIsCacheBounded()) {
            long maxAllocation =
                ((ByteBufferCache) bufMgr).getMaxAquiredBytes();
            limitToStopAllocation = (maxAllocation *
                                     percentOfMaxStopAllocation) / 100;
            limitToRestartAllocation = (maxAllocation *
                                        percentOfMaxRestartAllocation) / 100;
        } else {
            limitToStopAllocation = (DEFAULT_MAX_BYTES_ALLOCATION_LIMIT *
                                     percentOfMaxStopAllocation) / 100;
            limitToRestartAllocation = (DEFAULT_MAX_BYTES_ALLOCATION_LIMIT *
                                        percentOfMaxRestartAllocation) / 100;
        }
    }

    void startReading()
    {
        stopped = false;
    }

    public String toString()
    {
        return "InputChannel#" + id;
    }
}
