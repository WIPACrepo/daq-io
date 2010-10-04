package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Simplified output engine.
 */
public class SimpleOutputEngine
    implements DAQComponentObserver, DAQComponentOutputProcess, Runnable
{
    /** selector timeout in msec. */
    private static final int DEFAULT_SELECTOR_TIMEOUT_MSEC = 250;

    /** Error logger. */
    private static final Log LOG = LogFactory.getLog(SimpleOutputEngine.class);

    /** All possible states. */
    private enum State { STOPPED, RUNNING, DESTROYED, ERROR }

    /** Engine type. */
    private String engineType;
    /** Engine ID. */
    private int engineId;
    /** Engine function. */
    private String engineFunction;

    /** Current engine state. */
    private State state = State.STOPPED;

    /** Used to sequentially number channels. */
    private int nextChannelNum;
    /** List of active channels. */
    private ArrayList<SimpleOutputChannel> channelList =
        new ArrayList<SimpleOutputChannel>();
    /**
     * List of channels which have queued output
     * but are not registered with the selector.
     */
    private ArrayList<SimpleOutputChannel> regList =
        new ArrayList<SimpleOutputChannel>();

    /** Channel selector. */
    private Selector selector;

    /** Component observer. */
    private DAQComponentObserver observer;

    /** Has this engine been connected to one or more input channels? */
    private boolean isConnected = false;

    /** Total number of records sent. */
    private long totalSent;

    /**
     * Create an output engine.
     *
     * @param type engine type
     * @param id engine ID
     * @param fcn engine function
     */
    public SimpleOutputEngine(String type, int id, String fcn)
    {
        engineType = type;
        engineId = id;
        engineFunction = fcn;

        try {
            selector = Selector.open();
        } catch (IOException ioe) {
            throw new Error("Cannot create Selector");
        }
    }

    /**
     * Add a channel.
     *
     * @param channel output channel
     * @param bufMgr byte buffer manager
     */
    public QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                              IByteBufferCache bufMgr)
    {
        if (state != State.STOPPED) {
            throw new RuntimeException("Engine should be stopped, not " +
                                       getPresentState());
        }

        if (!(channel instanceof SelectableChannel)) {
            throw new Error("Channel is not selectable");
        }

        int chanNum = nextChannelNum++;

        String name = toString() + ":" + chanNum;

        SimpleOutputChannel outChan =
            new SimpleOutputChannel(this, name, channel, bufMgr);

        synchronized (channelList) {
            channelList.add(outChan);
        }

        isConnected = true;

        return outChan;
    }

    /**
     * Unimplemented.
     */
    public void clearError()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Connect the output channel to this output engine.
     *
     * @param bufMgr byte buffer manager
     * @param chan output channel
     * @param srcId **UNUSED**
     *
     * @throws IOException if there is a problem
     */
    public QueuedOutputChannel connect(IByteBufferCache bufMgr,
                                       WritableByteChannel chan, int srcId)
        throws IOException
    {
        return addDataChannel(chan, bufMgr);
    }

    /**
     * Destroy this engine.
     */
    public void destroyProcessor()
    {
        synchronized (channelList) {
            if (channelList.size() > 0) {
                for (SimpleOutputChannel outChan : channelList) {
                    try {
                        outChan.close();
                    } catch (IOException ioe) {
                        LOG.error("Cannot close " + outChan, ioe);
                    }
                }

                channelList.clear();
                handleEngineStop();
            }
        }

        state = State.DESTROYED;
    }

    /**
     * Disconnect all channels.
     */
    public void disconnect()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        if (isConnected) {
            synchronized (channelList) {
                boolean isRunning = state == State.RUNNING;
                if (channelList.size() > 0) {
                    for (SimpleOutputChannel outChan : channelList) {
                        if (isRunning) {
                            outChan.sendLastAndStop();
                        } else {
                            try {
                                outChan.close();
                            } catch (IOException ioe) {
                                LOG.error("Cannot close " + outChan, ioe);
                            }
                        }
                    }

                    if (!isRunning) {
                        channelList.clear();
                    }
                }
            }

            isConnected = false;
        }
    }

    /**
     * Force all channels to stop.
     */
    public void forcedStopProcessing()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        if (state != State.STOPPED) {
            synchronized (channelList) {
                if (channelList.size() == 0) {
                    handleEngineStop();
                } else {
                    for (SimpleOutputChannel outChan : channelList) {
                        outChan.sendLastAndStop();
                    }
                }
            }
        }
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
        if (channelList.size() == 0) {
            return null;
        }

        if (channelList.size() != 1) {
            throw new Error("Engine " + toString() +
                            " should only contain one channel, not " +
                            channelList.size());
        }

        return channelList.get(0);
    }

    /**
     * Get the depth of all output channel queues.
     *
     * @return number of records waiting to be written by each output channel
     */
    public long[] getDepth()
    {
        long[] depthList;

        synchronized (channelList) {
            depthList = new long[channelList.size()];

            int idx = 0;
            for (SimpleOutputChannel outChan : channelList) {
                depthList[idx++] = outChan.getDepth();
            }
        }

        return depthList;
    }

    /**
     * Return number of active channels.
     *
     * @return number of active channels
     */
    public int getNumberOfChannels()
    {
        return channelList.size();
    }

    /**
     * Get the present output engine state.
     *
     * @return engine state
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
     * Get the total number of records written.
     *
     * @return total number of records written
     */
    public long getTotalRecordsSent()
    {
        return totalSent;
    }

    /**
     * Get the number of records written by all output channel queues.
     *
     * @return number of records written by each output channel
     */
    public long[] getRecordsSent()
    {
        long[] sentList;

        synchronized (channelList) {
            sentList = new long[channelList.size()];

            int idx = 0;
            for (SimpleOutputChannel outChan : channelList) {
                sentList[idx++] = outChan.getRecordsSent();
            }
        }

        return sentList;
    }

    /**
     * Do necessary clean-up if engine has stopped.
     */
    private void handleEngineStop()
    {
        if (state == State.RUNNING) {
            if (observer != null) {
                observer.update(NormalState.STOPPED,
                                DAQCmdInterface.SOURCE);
            }

            state = State.STOPPED;
        }
    }

    /**
     * Has this engine been connected to any output channels?
     *
     * @return <tt>true</tt> if engine has been connected to a channel
     */
    public boolean isConnected()
    {
        return isConnected;
    }

    /**
     * Has this engine been destroyed?
     *
     * @return <tt>true</tt> if engine has been destroyed
     */
    public boolean isDestroyed()
    {
        return state == State.DESTROYED;
    }

    /**
     * Is this engine in an error state?
     *
     * @return <tt>true</tt> if engine has encountered an unrecoverable error
     */
    public boolean isError()
    {
        return state == State.ERROR;
    }

    /**
     * Is this engine running?
     *
     * @return <tt>true</tt> if engine is running
     */
    public boolean isRunning()
    {
        return state == State.RUNNING;
    }

    /**
     * Has this engine been stopped?
     *
     * @return <tt>true</tt> if engine is stopped
     */
    public boolean isStopped()
    {
        return state == State.STOPPED;
    }

    /**
     * When possible, register this channel with the selector.
     *
     * @param outChan output channel
     */
    private void registerChannel(SimpleOutputChannel outChan)
    {
        synchronized (regList) {
            regList.add(outChan);
            wakeup();
        }
   }

    /**
     * Observer to notify when this engine stops.
     *
     * @param compObserver observer
     */
    public void registerComponentObserver(DAQComponentObserver compObserver)
    {
        observer = compObserver;
    }

    /**
     * Remove a stopped channel from the channel list.
     *
     * @param chan stopped channel
     */
    private void removeStoppedChannel(SimpleOutputChannel chan)
    {
        synchronized (channelList) {
            int idx = channelList.indexOf(chan);
            if (idx < 0) {
                LOG.error("Couldn't find stopped channel " + chan);
            } else {
                channelList.remove(idx);
                try {
                    chan.close();
                } catch (IOException ioe) {
                    LOG.error("Couldn't close stopped channel", ioe);
                }
            }

            if (state == State.RUNNING && channelList.size() == 0) {
                handleEngineStop();
            }

            wakeup();
        }
    }

    /**
     * Main thread loop.
     */
    public void run()
    {
        totalSent = 0;

        while (state == State.RUNNING || channelList.size() > 0) {
            int numSelected = 0;
            try {
                numSelected = selector.select(DEFAULT_SELECTOR_TIMEOUT_MSEC);
            } catch (IOException ioe) {
                LOG.error("select() failed", ioe);
                break;
            }

            if (regList.size() > 0) {
                synchronized (regList) {
                    for (SimpleOutputChannel outChan : regList) {
                        try {
                            outChan.register(selector);
                        } catch (IOException ioe) {
                            LOG.error("Cannot register channel " + outChan,
                                      ioe);
                        }
                    }
                    regList.clear();
                }
            }

            if (numSelected == 0) {
                if (channelList.size() > 0) {
                    // check for stopped channels
                    synchronized (channelList) {
                        for (int i = 0; i < channelList.size(); ) {
                            SimpleOutputChannel chan = channelList.get(i);
                            if (chan.isStopped()) {
                                channelList.remove(i);
                                try {
                                    chan.close();
                                } catch (IOException ioe) {
                                    LOG.error("Couldn't close " + chan, ioe);
                                }
                            } else {
                                i++;
                            }
                        }

                        if (channelList.size() == 0) {
                            handleEngineStop();
                        }
                    }
                }
            } else {
                Iterator<SelectionKey> i = selector.selectedKeys().iterator();
                while (i.hasNext()) {
                    SelectionKey key = i.next();
                    i.remove();

                    SimpleOutputChannel chan =
                        ((SimpleOutputChannel) key.attachment());
                    if (chan.isStopped()) {
                        // XXX probably should keep channels around in case
                        // XXX we ever want to restart without reconnecting
                        removeStoppedChannel(chan);
                        key.cancel();
                    } else if (!chan.isOutputQueued()) {
                        chan.unregister(key);
                    } else {
                        chan.transmit();
                    }
                }
            }
        }

        if (state == State.RUNNING) {
            state = State.STOPPED;
        }
    }

    /**
     * Queue a stop message on all active output channels.
     */
    public void sendLastAndStop()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        synchronized (channelList) {
            if (channelList.size() == 0) {
                state = State.STOPPED;
            } else {
                for (SimpleOutputChannel outChan : channelList) {
                    outChan.sendLastAndStop();
                }
            }
        }

        wakeup();
    }

    /**
     * Do nothing.
     */
    public void start()
    {
        // nothing to do
    }

    /**
     * Start sending records.
     */
    public void startProcessing()
    {
        if (state != State.STOPPED) {
            throw new RuntimeException("Engine should be stopped, not " +
                                       getPresentState());
        }

        synchronized (channelList) {
            if (channelList.size() > 0) {
                for (SimpleOutputChannel outChan : channelList) {
                    outChan.startProcessing();
                }
            }
        }

        Thread thread = new Thread(this);
        thread.setName(engineType + "-" + engineId + "-" + engineFunction);
        thread.start();

        state = State.RUNNING;
    }

    /**
     * Debugging string.
     *
     * @return debugging string
     */
    public String toString()
    {
        return engineType + "#" + engineId + ":" + engineFunction +
            "=" + state +
            (channelList.size() == 0 ? "" : "*" + channelList.size());
    }

    /**
     * Unimplemented.
     */
    public void update(Object obj, String notificationID)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Wake the selector
     */
    private void wakeup()
    {
        selector.wakeup();
    }

    /**
     * Connection to input channel.
     */
    class SimpleOutputChannel
        implements QueuedOutputChannel
    {
        /** Number of bytes in 'stop' message'. */
        private static final int STOP_MESSAGE_SIZE = 4;

        /** Approximate maximum number of bytes to send at a time. */
        private static final int XMIT_GROUP_MAX_BYTES = 2048;

        /** Parent engine. */
        private SimpleOutputEngine parent;
        /** Channel name. */
        private String name;
        /** Actual output channel. */
        private WritableByteChannel channel;
        /** Byte buffer manager. */
        private IByteBufferCache bufferMgr;

        /** Queue of records to be written. */
        private LinkedList<ByteBuffer> outputQueue =
            new LinkedList<ByteBuffer>();

        /** Is this channel registered with the parent engine? */
        private boolean registered;

        /** Number of records sent. */
        private long numSent;
        /** <tt>True</tt> if this channel has been stopped. */
        private boolean stopped;

        /**
         * Create a simple output channel.
         *
         * @param parent parent output engine
         * @param name channel name
         * @param channel output channel
         * @param bufferMgr byte buffer manager
         */
        SimpleOutputChannel(SimpleOutputEngine parent, String name,
                            WritableByteChannel channel,
                            IByteBufferCache bufferMgr)
        {
            this.parent = parent;
            this.name = name;
            this.channel = channel;
            this.bufferMgr = bufferMgr;
        }

        /**
         * Close the connection.
         *
         * @throws IOException if there was an error
         */
        public void close()
            throws IOException
        {
            WritableByteChannel tmpChan = channel;
            channel = null;
            tmpChan.close();
        }

        /**
         * Unimplemented.
         */
        public void destinationClosed()
        {
            throw new Error("Unimplemented");
        }

        /**
         * Wake the parent so it starts telling me to write data.
         */
        public void flushOutQueue()
        {
            if (outputQueue.size() > 0) {
                parent.wakeup();
            }
        }

        /**
         * Get the depth of the output queue.
         *
         * @return number of records waiting to be written
         */
        public long getDepth()
        {
            return outputQueue.size();
        }

        /**
         * Get the number of records sent by this channel.
         *
         * @return number of records sent
         */
        public long getRecordsSent()
        {
            return numSent;
        }

        /**
         * Are there records waiting to be written?
         *
         * @return <tt>true</tt> if the output queue is not empty
         */
        public boolean isOutputQueued()
        {
            return !outputQueue.isEmpty() ;
        }

        /**
         * Has this channel been stopped?
         *
         * @return <tt>true</tt> if the channel has been stopped
         */
        boolean isStopped()
        {
            return stopped;
        }

        /**
         * Add this record to the queue.
         *
         * @param buf new record buffer
         */
        public void receiveByteBuffer(ByteBuffer buf)
        {
            if (stopped) {
                LOG.error("Queuing buffer after channel has been stopped");
            } else if (parent.isStopped()) {
                LOG.error("Queuing buffer after engine has stopped");
            }

            synchronized (outputQueue) {
                outputQueue.add(buf);

                if (!registered) {
                    parent.registerChannel(this);
                    registered = true;
                }
            }
        }

        /**
         * Register this channel with the selector.
         *
         * @param sel selector
         *
         * @throws IOException if there is a problem registering the channel
         */
        void register(Selector sel)
            throws IOException
        {
            if (channel != null) {
                ((SelectableChannel) channel).register(sel,
                                                       SelectionKey.OP_WRITE,
                                                       this);
                registered = true;
            }
        }

        /**
         * Do nothing.
         *
         * @param observer observer object
         * @param notificationID identifier for this observer
         */
        public void registerComponentObserver(DAQComponentObserver observer,
                                              String notificationID)
        {
            // do nothing
        }

        /**
         * Add a "stop" message to the output queue.
         */
        public void sendLastAndStop()
        {
            if (!stopped) {
                ByteBuffer stopMessage = ByteBuffer.allocate(STOP_MESSAGE_SIZE);
                stopMessage.putInt(0, STOP_MESSAGE_SIZE);
                receiveByteBuffer(stopMessage);
            }
        }

        /**
         * Start processing output.
         */
        void startProcessing()
        {
            // make sure the channel is non-blocking
            if (channel instanceof SelectableChannel) {
                SelectableChannel selChan = (SelectableChannel) channel;
                if (selChan.isBlocking()) {
                    try {
                        selChan.configureBlocking(false);
                        LOG.error("Configured non-blocking for " + toString());
                    } catch (IOException ioe) {
                        LOG.error("Could not configure non-blocking for " +
                                  toString(), ioe);
                    }
                }
            }
        }

        /**
         * Stop processing output.
         */
        void stopProcessing()
        {
            stopped = true;

            parent.wakeup();
        }

        /**
         * Transmit as many records as possible.
         */
        void transmit()
        {
            int bytesLeft = XMIT_GROUP_MAX_BYTES;
            while (true) {
                ByteBuffer buf;
                synchronized (outputQueue) {
                    buf = outputQueue.remove(0);
                }

                final int payLen = buf.getInt(0);

                if (stopped) {
                    if (payLen != STOP_MESSAGE_SIZE) {
                        LOG.error("Channel " + name + " saw " + payLen +
                                  "-byte payload after stop");
                        bytesLeft = 0;
                        break;
                    }
                } else if (channel == null) {
                    LOG.error("Channel " + name + " saw " + payLen +
                              "-byte payload after close");
                    break;
                } else {
                    buf.position(0);
                    buf.limit(payLen);

                    int numWritten = 0;
                    while (numWritten < payLen) {
                        try {
                            int bytes = channel.write(buf);
                            if (bytes < 0) {
                                LOG.error("Channel " + name + " write of " +
                                          payLen + " bytes returned " + bytes);
                                break;
                            }
                            numWritten += bytes;
                        } catch (IOException ioe) {
                            LOG.error("Channel " + name + " write failed", ioe);
                            break;
                        }
                    }

                    bytesLeft -= numWritten;

                    numSent++;
                    totalSent++;
                }

                if (payLen == STOP_MESSAGE_SIZE) {
                    stopProcessing();
                } else if (bufferMgr != null) {
                    bufferMgr.returnBuffer(buf);
                }

                // if we're out of payloads, or if we couldn't send the most
                // recent payload again, stop sending
                if (outputQueue.size() == 0 || bytesLeft < payLen) {
                    break;
                }
            }
        }

        /**
         * Debugging string.
         *
         * @return debugging string
         */
        public String toString()
        {
            return name + (outputQueue.size() == 0 ? "" :
                           "*" + outputQueue.size()) +
                (stopped ? "*STOPPED" : "");
        }

        /**
         * Remove this channel from the selector.
         *
         * @param key selector key corresponding to this channel
         */
        void unregister(SelectionKey key)
        {
            synchronized (outputQueue) {
                if (outputQueue.size() == 0) {
                    key.cancel();
                    registered = false;
                }
            }
        }
    }
}
