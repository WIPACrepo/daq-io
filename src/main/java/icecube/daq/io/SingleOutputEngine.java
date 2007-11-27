package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadOutput;
import icecube.daq.payload.IWriteablePayload;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private SimpleOutputChannel outChan;
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
    public OutputChannel addDataChannel(WritableByteChannel channel,
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
            new SimpleOutputChannel(this, name, channel, bufMgr);

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
    public OutputChannel connect(IByteBufferCache bufMgr,
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
