package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadOutput;
import icecube.daq.payload.IWriteablePayload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Threaded output channel.
 */
class SingleOutputChannel
    implements QueuedOutputChannel, Runnable
{
    private static final Log LOG = LogFactory.getLog(SingleOutputChannel.class);

    static final int STOP_MESSAGE_SIZE = 4;

    private IOChannelParent parent;
    private String name;
    private WritableByteChannel channel;
    private IByteBufferCache bufferMgr;
    private ArrayList<ByteBuffer> outputQueue;

    private Thread thread;

    private ByteBuffer stopMessage;

    private long bytesSent;
    private long numSent;

    /**
     * Create an output channel.
     *
     * @param parent parent output engine
     * @param name channel name
     * @param channel actual channel
     * @param bufferMgr ByteBuffer allocation manager
     */
    SingleOutputChannel(IOChannelParent parent, String name,
                        WritableByteChannel channel, IByteBufferCache bufferMgr)
    {
        this.parent = parent;
        this.name = name;
        this.channel = channel;
        this.bufferMgr = bufferMgr;

        this.outputQueue = new ArrayList<ByteBuffer>();

        // build stop message
        stopMessage = ByteBuffer.allocate(STOP_MESSAGE_SIZE);
        stopMessage.putInt(0, STOP_MESSAGE_SIZE);
    }

    /**
     * Close the connection.
     *
     * @throws IOException if there was an error
     */
    void close()
        throws IOException
    {
        stopProcessing();

        WritableByteChannel tmpChan = channel;
        channel = null;

        tmpChan.close();
    }

    public void destinationClosed()
    {
        flushOutQueue();
    }

    /**
     * Wake output thread in the hope that it will clear the output queue.
     */
    public void flushOutQueue()
    {
        if (isRunning()) {
            // wake the thread in case its sleeping
            synchronized (outputQueue) {
                outputQueue.notify();
            }
        }
    }

    /**
     * Get the total number of bytes sent.
     *
     * @return number of bytes sent
     */
    public long getBytesSent()
    {
        return bytesSent;
    }

    /**
     * Get the current output queue depth.
     *
     * @return output queue depth
     */
    public long getDepth()
    {
        return outputQueue.size();
    }

    /**
     * Get the total number of payload records sent.
     *
     * @return number of records sent
     */
    public long getRecordsSent()
    {
        return numSent;
    }

    /**
     * Is there data in the output queue?
     *
     * @return <tt>true</tt> if there is queued data
     */
    public boolean isOutputQueued()
    {
        return outputQueue.size() > 0;
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

    /**
     * Add data to the output queue.
     *
     * @param buf buffer to be added
     */
    public void receiveByteBuffer(ByteBuffer buf)
    {
        if (!isRunning()) {
            throw new Error(name + " thread is not running");
        }

        synchronized (outputQueue) {
            outputQueue.add(buf);
            outputQueue.notify();
        }
    }

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

    /**
     * Output thread.
     */
    public void run()
    {
        Selector selector = null;
        boolean sentSelError = false;

        while (isRunning()) {
            ByteBuffer outBuf;
            synchronized (outputQueue) {
                while (isRunning() && outputQueue.size() == 0) {
                    try {
                        outputQueue.wait();
                    } catch (InterruptedException ie) {
                        // ignore interrupts
                    }
                }

                if (!isRunning()) {
                    break;
                }

                outBuf = outputQueue.remove(0);
            }

            int numBytes = 0;
            while (true) {
                try {
                    numBytes += channel.write(outBuf);
                } catch (IOException ioe) {
                    parent.channelError(this, outBuf, ioe);
                    break;
                }

                if (numBytes == outBuf.limit()) {
                    break;
                }


                if (selector == null) {
                    try {
                        selector = Selector.open();
                        SelectableChannel selChan = (SelectableChannel) channel;
                        selChan.register(selector, selChan.validOps());
                    } catch (IOException ioe) {
                        if (!sentSelError) {
                            parent.channelError(this, null, ioe);
                            sentSelError = true;
                        }

                        thread = null;
                        selector = null;
                        continue;
                    }
                }

                boolean notWriteable = true;
                while (notWriteable) {
                    try {
                        selector.select();
                    } catch (IOException ioe) {
                        LOG.error(this.toString() + " selector had problem",
                                  ioe);
                        break;
                    }

                    for (SelectionKey key : selector.selectedKeys()) {
                        notWriteable = !(key.isValid() && key.isWritable());
                    }
                    selector.selectedKeys().clear();
                }
            }

            if (outBuf.capacity() == STOP_MESSAGE_SIZE &&
                outBuf.getInt(0) == STOP_MESSAGE_SIZE)
            {
                break;
            }

            if (bufferMgr != null) {
                bufferMgr.returnBuffer(outBuf);
            }

            bytesSent += outBuf.getInt(0);
            numSent++;
        }

        parent.channelStopped(this);

        thread = null;
    }

    /**
     * Add a stop message to the output queue.
     */
    public void sendLastAndStop() {
        receiveByteBuffer(stopMessage);
    }

    /**
     * Start the output thread.
     */
    void startProcessing()
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

        synchronized (outputQueue) {
            outputQueue.notify();
        }
    }

    /**
     * String representation of output channel.
     *
     * @return debugging string
     */
    public String toString()
    {
        return name + (outputQueue.size() == 0 ? "" : "*" + outputQueue.size());
    }
}

/**
 * Manage output to a single channel.
 */
public class SingleOutputEngine
    implements DAQComponentOutputProcess, IOChannelParent, IPayloadOutput
{
    private static final Log LOG = LogFactory.getLog(SingleOutputEngine.class);

    private enum State { STOPPED, RUNNING, DESTROYED, ERROR }

    private String engineType;
    private int engineId;
    private String engineFunction;

    private State state = State.STOPPED;

    private SingleOutputChannel outChan;
    private int nextChannelNum;

    private DAQComponentObserver observer;

    private boolean isConnected = false;

    /**
     * Create a single-channel output engine.
     *
     * @param type engine type
     * @param id engine id
     * @param fcn engine function
     */
    public SingleOutputEngine(String type, int id, String fcn)
    {
        engineType = type;
        engineId = id;
        engineFunction = fcn;
    }

    /**
     * Add the data channel.  Note that only one channel at a time can
     * be active.
     *
     * @param channel data channel
     * @param bufMgr allocation manager to which completed buffers are returned
     */
    public QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                              IByteBufferCache bufMgr)
    {
        if (outChan != null) {
            throw new Error("Channel has already been added");
        }

        int chanNum = nextChannelNum++;

        String name;
        if (engineType == null) {
            name = "SingleOutputEngine:" + chanNum;
        } else {
            name = engineType + "#" + engineId + ":" + engineFunction +
                ":" + chanNum;
        }

        outChan =
            new SingleOutputChannel(this, name, channel, bufMgr);

        isConnected = true;

        return outChan;
    }

    /**
     * Callback used by the channel to report errors.
     *
     * @param chan data channel in which the error happened
     * @param buf buffer which caused the error (may be <tt>null</tt>)
     * @param ex exception
     */
    public void channelError(IOChannel chan, ByteBuffer buf, Exception ex)
    {
        LOG.error("Channel " + chan + " couldn't send buffer", ex);
    }

    /**
     * Callback used by the channel to report that it is exiting.
     *
     * @param chan data channel which is finished
     */
    public void channelStopped(IOChannel chan)
    {
        if (outChan != null) {
            if (chan != outChan) {
                LOG.error("Couldn't find stopped channel " + chan);
            } else {
                state = State.STOPPED;

                if (observer != null) {
                    observer.update(NormalState.STOPPED,
                                    DAQCmdInterface.SOURCE);
                }
            }

            outChan = null;
        }
    }

    /**
     * Unimplemented.
     */
    public void clearError()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Unimplemented.
     */
    public void close()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Connect the specified channel -- this is merely a wrapper around
     * <tt>addDataChannel()</tt>.
     *
     * @param bufMgr allocation manager
     * @param chan data channel
     * @param srcId source ID of remote end
     */
    public QueuedOutputChannel connect(IByteBufferCache bufMgr,
                                       WritableByteChannel chan, int srcId)
        throws IOException
    {
        return addDataChannel(chan, bufMgr);
    }

    /**
     * Destroy this object.  When this method returns, the engine will
     * no longer be useable.
     */
    public void destroyProcessor()
    {
        forcedStopProcessing();

        state = State.DESTROYED;
    }

    /**
     * Disconnect the active data channel.
     *
     * @throws IOException if the data channel could not be stopped
     */
    public void disconnect()
        throws IOException
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        IOException ioEx = null;

        if (isConnected) {
            if (outChan != null) {
                try {
                    outChan.close();
                } catch (IOException ioe) {
                    ioEx = ioe;
                }

                outChan = null;
            }

            isConnected = false;
        }

        if (ioEx != null) {
            throw ioEx;
        }

        state = State.STOPPED;
    }

    /**
     * Stop the data channel.
     */
    public void forcedStopProcessing()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        if (outChan == null) {
            state = State.STOPPED;
        } else {
            outChan.stopProcessing();
        }
    }

    /**
     * Get the number of data bytes sent.
     *
     * @return number of bytes sent.
     */
    public long getBytesSent()
    {
        if (outChan == null) {
            return -1L;
        }

        return outChan.getBytesSent();
    }

    /**
     * Return the single channel associated with this output engine.
     *
     * @return output channel
     *
     * @throws Error if there is more than one output channel
     */
    public OutputChannel getChannel()
    {
        return outChan;
    }

    /**
     * Get the number of queued buffers.
     *
     * @return number of buffers waiting to be written
     */
    public long getDepth()
    {
        if (outChan == null) {
            return -1L;
        }

        return outChan.getDepth();
    }

    /**
     * Get the current engine state.
     *
     * @return name of current state
     */
    public String getPresentState()
    {
        switch (state) {
        case STOPPED:
            return "Idle/Stopped";
        case RUNNING:
            return "Running";
        case DESTROYED:
            return "Destroyed";
        case ERROR:
            return "Error";
        default:
            break;
        }

        return "?Unknown?";
    }

    /**
     * Get the number of data records sent.
     *
     * @return number of records sent.
     */
    public long getRecordsSent()
    {
        if (outChan == null) {
            return -1L;
        }

        return outChan.getRecordsSent();
    }

    /**
     * Is the engine connected to a remote component?
     *
     * @return <tt>true</tt> if the data channel is connected to a
     *         remote receiver
     */
    public boolean isConnected()
    {
        return isConnected;
    }

    /**
     * Has the engine been destroyed?
     *
     * @return <tt>true</tt> if the engine is no longer useable
     */
    public boolean isDestroyed()
    {
        return state == State.DESTROYED;
    }

    /**
     * Is the engine in an error state?
     *
     * @return <tt>true</tt> if the engine has encountered an error
     */
    public boolean isError()
    {
        return state == State.ERROR;
    }

    /**
     * Is the engine able to send data?
     *
     * @return <tt>true</tt> if there is an active data channel
     */
    public boolean isRunning()
    {
        return state == State.RUNNING;
    }

    /**
     * Is the engine stopped?
     *
     * @return <tt>true</tt> if there is no data channel
     */
    public boolean isStopped()
    {
        return state == State.STOPPED;
    }

    /**
     * Register an observer.
     *
     * @param compObserver observer
     */
    public void registerComponentObserver(DAQComponentObserver compObserver)
    {
        observer = compObserver;
    }

    /**
     * Does nothing
     *
     * @param bufMgr buffer manager
     */
    public void registerBufferManager(IByteBufferCache bufMgr)
    {
        // do nothing
    }

    /**
     * Add a stop message to the data channel's queue.
     */
    public void sendLastAndStop()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        if (outChan == null) {
            state = State.STOPPED;
        } else {
            outChan.sendLastAndStop();
        }
    }

    /**
     * Does nothing.
     */
    public void start()
    {
        // do nothing do
    }

    /**
     * Start the data channel sending data.
     */
    public void startProcessing()
    {
        if (state != State.STOPPED) {
            throw new RuntimeException("Engine should be stopped, not " +
                                       getPresentState());
        }

        if (outChan == null) {
            throw new Error("Output channel has not been set");
        }

        outChan.startProcessing();

        state = State.RUNNING;
    }

    /**
     * Wrapper for <tt>forcedStopProcessing</tt>.
     */
    public void stop()
    {
        forcedStopProcessing();
    }

    /**
     * Write the payload.
     *
     * @param payload payload
     *
     * @return number of bytes written
     */
    public int writePayload(IWriteablePayload payload)
        throws IOException
    {
        int len = payload.getPayloadLength();
        ByteBuffer buf = ByteBuffer.allocate(len);
        if (buf == null) {
            throw new Error("Could not allocate " + len + "-byte buffer");
        }

        buf.clear();
        int outLen = payload.writePayload(false, 0, buf);
        if (len != outLen) {
            throw new Error("Expected to load " + len + " bytes, but loaded " +
                            outLen);
        }

        buf.position(0);
        outChan.receiveByteBuffer(buf);

        return outLen;
    }
}
