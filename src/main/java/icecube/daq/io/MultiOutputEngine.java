package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadOutput;
import icecube.daq.payload.IWriteablePayload;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class ThreadedOutputChannel
    implements OutputChannel, Runnable
{
    private static final Log LOG =
        LogFactory.getLog(ThreadedOutputChannel.class);

    static final int STOP_MESSAGE_SIZE = 4;

    private MultiOutputEngine parent;
    private String name;
    private WritableByteChannel channel;
    private IByteBufferCache bufferMgr;
    private ArrayList<ByteBuffer> outputQueue;

    private Thread thread;

    private ByteBuffer stopMessage;

    private long numSent;

    ThreadedOutputChannel(MultiOutputEngine parent, String name,
                          WritableByteChannel channel,
                          IByteBufferCache bufferMgr)
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

    public void flushOutQueue()
    {
        if (isRunning()) {
            // wake the thread in case its sleeping
            synchronized (outputQueue) {
                outputQueue.notify();
            }
        }
    }

    public long getDepth()
    {
        return outputQueue.size();
    }

    public long getRecordsSent()
    {
        return numSent;
    }

    public boolean isOutputQueued()
    {
        return outputQueue.size() > 0;
    }

    boolean isRunning()
    {
        return thread != null;
    }

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

    public void registerComponentObserver(DAQComponentObserver compObserver,
                                          String notificationID)
    {
        throw new Error("Unimplemented");
    }

    public void run()
    {
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

            if (channel == null) {
                break;
            }

            try {
                channel.write(outBuf);
            } catch (IOException ioe) {
                parent.channelSendError(this, outBuf, ioe);
            }

            if (outBuf.capacity() == STOP_MESSAGE_SIZE &&
                outBuf.getInt(0) == STOP_MESSAGE_SIZE)
            {
                break;
            }

            if (bufferMgr != null) {
                bufferMgr.returnBuffer(outBuf);
            }

            numSent++;
        }

        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ioe) {
                LOG.error("Ignoring " + toString() + " close exception", ioe);
            }
        }

        parent.channelStopped(this);

        thread = null;
    }

    void sendLastAndStop() {
        receiveByteBuffer(stopMessage);
    }

    void startProcessing()
    {
        thread = new Thread(this);
        thread.setName(name);
        thread.start();
    }

    void stopProcessing()
    {
        thread = null;

        synchronized (outputQueue) {
            outputQueue.notify();
        }
    }

    public String toString()
    {
        return name + (outputQueue.size() == 0 ? "" : "*" + outputQueue.size());
    }
}

public class MultiOutputEngine
    implements DAQComponentOutputProcess, IPayloadOutput
{
    private static final Log LOG = LogFactory.getLog(MultiOutputEngine.class);

    private static final int STOP_MESSAGE_SIZE =
        ThreadedOutputChannel.STOP_MESSAGE_SIZE;

    private enum State { STOPPED, RUNNING, DESTROYED, ERROR }

    private String engineType;
    private int engineId;
    private String engineFunction;

    //private Thread thread;

    private State state = State.STOPPED;

    private ArrayList<ThreadedOutputChannel> channelList;
    private int nextChannelNum;

    private DAQComponentObserver observer;

    private boolean isConnected = false;

    public MultiOutputEngine(String type, int id, String fcn)
    {
        engineType = type;
        engineId = id;
        engineFunction = fcn;

        channelList = new ArrayList<ThreadedOutputChannel>();
    }

    /** DAQComponentOutputProcess method */
    public OutputChannel addDataChannel(WritableByteChannel channel,
                                        IByteBufferCache bufMgr)
    {
        int chanNum = nextChannelNum++;

        String name;
        if (engineType == null) {
            name = "MultiOutputEngine:" + chanNum;
        } else {
            name = engineType + "#" + engineId + ":" + engineFunction +
                ":" + chanNum;
        }

        ThreadedOutputChannel outChan =
            new ThreadedOutputChannel(this, name, channel, bufMgr);

        synchronized (channelList) {
            channelList.add(outChan);
        }

        isConnected = true;

        return outChan;
    }

    void channelSendError(ThreadedOutputChannel chan, ByteBuffer buf,
                          IOException ioe)
    {
        LOG.error("Channel " + chan + " couldn't send buffer", ioe);
    }

    void channelStopped(ThreadedOutputChannel chan)
    {
        synchronized (channelList) {
            int idx = channelList.indexOf(chan);
            if (idx < 0) {
                LOG.error("Couldn't find stopped channel " + chan);
            } else {
                channelList.remove(idx);
            }

            if (channelList.size() == 0) {
                if (observer != null) {
                    observer.update(NormalState.STOPPED,
                                    DAQCmdInterface.SOURCE);
                }

                state = State.STOPPED;
            }

            channelList.notify();
        }
    }

    public void clearError()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Close the connection.
     */
    public void close()
    {
        throw new Error("Unimplemented");
    }

    /** DAQComponentOutputProcess method */
    public OutputChannel connect(IByteBufferCache bufMgr,
                                 WritableByteChannel chan, int srcId)
        throws IOException
    {
        return addDataChannel(chan, bufMgr);
    }

    /** DAQComponentIOProcess method */
    public void destroyProcessor()
    {
        forcedStopProcessing();

        state = State.DESTROYED;
    }

    /** DAQComponentOutputProcess method */
    public void disconnect()
        throws IOException
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        IOException ioEx = null;

        if (isConnected) {
            synchronized (channelList) {
                if (channelList.size() > 0) {
                    for (ThreadedOutputChannel outChan : channelList) {
                        try {
                            outChan.close();
                        } catch (IOException ioe) {
                            if (ioEx == null) {
                                ioEx = ioe;
                            }
                        }
                    }
                }
            }

            isConnected = false;
        }

        if (ioEx != null) {
            throw ioEx;
        }
    }

    /** DAQComponentIOProcess method */
    public void forcedStopProcessing()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        synchronized (channelList) {
            if (channelList.size() > 0) {
                for (ThreadedOutputChannel outChan : channelList) {
                    outChan.stopProcessing();
                }
            }
        }
    }

    public long[] getDepth()
    {
        long[] depthList;

        synchronized (channelList) {
            depthList = new long[channelList.size()];

            int idx = 0;
            for (ThreadedOutputChannel outChan : channelList) {
                depthList[idx++] = outChan.getDepth();
            }
        }

        return depthList;
    }

    /** DAQComponentIOProcess method */
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

    public long[] getRecordsSent()
    {
        long[] sentList;

        synchronized (channelList) {
            sentList = new long[channelList.size()];

            int idx = 0;
            for (ThreadedOutputChannel outChan : channelList) {
                sentList[idx++] = outChan.getRecordsSent();
            }
        }

        return sentList;
    }

    /** DAQComponentOutputProcess method */
    public boolean isConnected()
    {
        return isConnected;
    }

    /** DAQComponentIOProcess method */
    public boolean isDestroyed()
    {
        return state == State.DESTROYED;
    }

    public boolean isError()
    {
        return state == State.ERROR;
    }

    /** DAQComponentIOProcess method */
    public boolean isRunning()
    {
        return state == State.RUNNING;
    }

    /** DAQComponentIOProcess method */
    public boolean isStopped()
    {
        return state == State.STOPPED;
    }

    /** DAQComponentIOProcess method */
    public void registerComponentObserver(DAQComponentObserver compObserver)
    {
        observer = compObserver;
    }

    /**
     * Unused
     *
     * @param bufMgr buffer manager
     */
    public void registerBufferManager(IByteBufferCache bufMgr)
    {
        // do nothing
    }

    /** DAQComponentOutputProcess method */
    public void sendLastAndStop()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        synchronized (channelList) {
            if (channelList.size() > 0) {
                for (ThreadedOutputChannel outChan : channelList) {
                    outChan.sendLastAndStop();
                }
            }
        }
    }

    /** DAQComponentIOProcess method */
    public void start()
    {
        // nothing to do
    }

    /** DAQComponentIOProcess method */
    public void startProcessing()
    {
        if (state != State.STOPPED) {
            throw new RuntimeException("Engine should be stopped, not " +
                                       getPresentState());
        }

        synchronized (channelList) {
            for (ThreadedOutputChannel outChan : channelList) {
                outChan.startProcessing();
            }
        }

        state = State.RUNNING;
    }

    /**
     * Stop writing payloads.
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
    {
        throw new Error("Unimplemented");
    }
}
