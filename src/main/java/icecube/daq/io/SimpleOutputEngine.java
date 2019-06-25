package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.impl.SourceID;

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
    /** Maximum channel depth */
    private int maxChannelDepth;

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

    /** Number of records sent. */
    private long numSent;
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
        this(type, id, fcn, Integer.MAX_VALUE);
    }

    /**
     * Create an output engine.
     *
     * @param type engine type
     * @param id engine ID
     * @param fcn engine function
     * @param maxChanDepth maximum depth (new data for a channel will not be
     *                     read if it contains more than maxChanDepth items)
     */
    public SimpleOutputEngine(String type, int id, String fcn,
                              int maxChanDepth)
    {
        engineType = type;
        engineId = id;
        engineFunction = fcn;
        maxChannelDepth = maxChanDepth;

        try {
            selector = Selector.open();
        } catch (IOException ioe) {
            throw new Error("Cannot create Selector", ioe);
        }
    }

    /**
     * Add a channel.
     *
     * @param channel output channel
     * @param bufMgr byte buffer manager
     * @param name channel name (used in logging/exception messages)
     */
    @Override
    public QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                              IByteBufferCache bufMgr,
                                              String name)
    {
        if (state != State.STOPPED) {
            throw new RuntimeException("Engine should be stopped, not " +
                                       getPresentState());
        }

        if (!(channel instanceof SelectableChannel)) {
            throw new Error("Channel is not selectable");
        }

        if (name == null) {
            name = Integer.toString(nextChannelNum++);
        }

        String fullName = engineFunction + ":" + name;

        SimpleOutputChannel outChan =
            new PayloadStopChannel(this, fullName, channel, bufMgr,
                                   maxChannelDepth);

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
     * @param srcId source ID of connecting channel
     *
     * @throws IOException if there is a problem
     */
    @Override
    public QueuedOutputChannel connect(IByteBufferCache bufMgr,
                                       WritableByteChannel chan, int srcId)
        throws IOException
    {
        return addDataChannel(chan, bufMgr, new SourceID(srcId).toString());
    }

    /**
     * Destroy this engine.
     */
    @Override
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

        if (selector != null) {
            Selector tmpSel = selector;
            selector = null;

            try {
                tmpSel.close();
            } catch (IOException ioe) {
                LOG.error("Cannot close selector", ioe);
            }
        }

        if (observer != null) {
            observer.update(NormalState.DESTROYED, DAQCmdInterface.SOURCE);
        }

        state = State.DESTROYED;
    }

    /**
     * Disconnect all channels.
     */
    @Override
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
    @Override
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
    @Override
    public OutputChannel getChannel()
    {
        synchronized (channelList) {
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
    @Override
    public int getNumberOfChannels()
    {
        return channelList.size();
    }

    /**
     * Get the present output engine state.
     *
     * @return engine state
     */
    @Override
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
     * Get the number of records written by all output channel queues.
     *
     * @return number of records written by each output channel
     */
    public long[] getChannelRecordsSent()
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
     * Get the total number of records written.
     *
     * @return total number of records written
     */
    @Override
    public long getRecordsSent()
    {
        return numSent;
    }

    /**
     * Get the total number of records written.
     *
     * @return total number of records written
     */
    @Override
    public long getTotalRecordsSent()
    {
        return totalSent;
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
    @Override
    public boolean isConnected()
    {
        return isConnected;
    }

    /**
     * Has this engine been destroyed?
     *
     * @return <tt>true</tt> if engine has been destroyed
     */
    @Override
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
    @Override
    public boolean isRunning()
    {
        return state == State.RUNNING;
    }

    /**
     * Has this engine been stopped?
     *
     * @return <tt>true</tt> if engine is stopped
     */
    @Override
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
            if (selector == null) {
                throw new Error("Engine has been closed; cannot register " +
                                outChan);
            }

            regList.add(outChan);
            wakeup();
        }
   }

    /**
     * Observer to notify when this engine changes state.
     *
     * @param compObserver observer
     */
    @Override
    public void registerComponentObserver(DAQComponentObserver compObserver)
    {
        if (compObserver != null && observer != null) {
            LOG.error("Overriding observer " + observer + " with new " +
                      compObserver);
        }

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
                    LOG.error("Couldn't close stopped channel " + chan, ioe);
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
    @Override
    public void run()
    {
        numSent = 0;

        // stop immediately if there are no channels
        if (channelList.size() > 0) {
            if (observer != null) {
                observer.update(NormalState.RUNNING, DAQCmdInterface.SOURCE);
            }

            state = State.RUNNING;
        } else {
            if (observer != null) {
                observer.update(NormalState.STOPPED, DAQCmdInterface.SOURCE);
            }

            state = State.STOPPED;
        }

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
            if (observer != null) {
                observer.update(NormalState.STOPPED, DAQCmdInterface.SOURCE);
            }

            state = State.STOPPED;
        }
    }

    /**
     * Queue a stop message on all active output channels.
     */
    @Override
    public void sendLastAndStop()
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        synchronized (channelList) {
            if (channelList.size() == 0) {
                if (observer != null) {
                    observer.update(NormalState.STOPPED,
                                    DAQCmdInterface.SOURCE);
                }

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
    @Override
    public void start()
    {
        // nothing to do
    }

    /**
     * Start sending records.
     */
    @Override
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
    }

    /**
     * Debugging string.
     *
     * @return debugging string
     */
    @Override
    public String toString()
    {
        return engineType + "#" + engineId + ":" + engineFunction +
            "=" + state +
            (channelList.size() == 0 ? "" : "*" + channelList.size());
    }

    /**
     * Unimplemented.
     */
    @Override
    public void update(Object obj, String notificationID)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Wake the selector
     */
    private void wakeup()
    {
        if (selector != null) {
            selector.wakeup();
        }
    }

    /**
     * Connection to input channel.
     */
    abstract class SimpleOutputChannel
        implements QueuedOutputChannel
    {
        /** Approximate maximum number of bytes to send at a time. */
        private static final int XMIT_GROUP_MAX_BYTES = 2048;
        /** Amount of time to sleep when channel exceeds 'maxDepth' */
        private static final int SLEEP_USEC = 1000;

        /** Parent engine. */
        private SimpleOutputEngine parent;
        /** Channel name. */
        private String name;
        /** Actual output channel. */
        private WritableByteChannel channel;
        /** Byte buffer manager. */
        private IByteBufferCache bufferMgr;
        /** Maximum channel depth */
        private int maxDepth;

        /** Queue of records to be written. */
        private LinkedList<ByteBuffer> outputQueue =
            new LinkedList<ByteBuffer>();

        /** Is this channel registered with the parent engine? */
        private boolean registered;

        /** Number of records sent by this channel. */
        private long chanSent;
        /** <tt>True</tt> if this channel has been stopped. */
        private boolean stopped;

        /** <tt>true</tt> if the remote end of this channel has been closed */
        private boolean brokenPipe;

        /** Count of post-stop buffers queued */
        private int numPostStopData;

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
                            IByteBufferCache bufferMgr, int maxDepth)
        {
            this.parent = parent;
            this.name = name;
            this.channel = channel;
            this.bufferMgr = bufferMgr;
            this.maxDepth = maxDepth;
        }

        /**
         * Close the connection.
         *
         * @throws IOException if there was an error
         */
        public void close()
            throws IOException
        {
            channel.close();
        }

        /**
         * Wake the parent so it starts telling me to write data.
         */
        @Override
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

        abstract int getRecordLength(ByteBuffer buf);

        /**
         * Get the number of records sent by this channel.
         *
         * @return number of records sent
         */
        public long getRecordsSent()
        {
            return chanSent;
        }

        /**
         * Are there records waiting to be written?
         *
         * @return <tt>true</tt> if the output queue is not empty
         */
        @Override
        public boolean isOutputQueued()
        {
            return !outputQueue.isEmpty() ;
        }

        /**
         * Is this a stop message?
         */
        abstract boolean isStopMessage(ByteBuffer buf, int payLen);

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
         * Add a "stop" message to the output queue.
         */
        abstract void queueStopMessage();

        /**
         * Add this record to the queue.
         *
         * @param buf new record buffer
         */
        @Override
        public void receiveByteBuffer(ByteBuffer buf)
        {
            final int warningFrequency = 10000;
            if (stopped) {
                if (++numPostStopData % warningFrequency == 1) {
                    LOG.error("Queuing " + buf.limit() +
                              "-byte buffer after channel " + name +
                              " has been stopped (num=" + numPostStopData +
                              ")");
                }
            } else if (parent.isStopped()) {
                if (++numPostStopData % warningFrequency == 1) {
                    LOG.error("Queuing " + buf.limit() + "-byte buffer after" +
                              " engine " + parent + " has stopped (num=" +
                              numPostStopData + ")");
                }
            }

            synchronized (outputQueue) {
                boolean warned = false;
                while (outputQueue.size() > maxDepth) {
                    if (!warned) {
                        LOG.error("Pausing " + parent + ":" + name +
                                  " queue (depth=" + outputQueue.size() +
                                  ", maxDepth=" + maxDepth + ")");
                        warned = true;
                    }
                    try {
                        // IC86 sees ~2600 requests per second
                        Thread.sleep(SLEEP_USEC);
                    } catch (InterruptedException iex) {
                    }
                }
                if (warned) {
                    LOG.error("Resuming " + parent + ":" + name +
                              " queue (maxDepth=" + maxDepth);
                }

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
            if (channel.isOpen()) {
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
        @Override
        public void registerComponentObserver(DAQComponentObserver observer,
                                              String notificationID)
        {
            throw new Error("Unimplemented");
        }

        /**
         * If the channel hasn't stopped, add a "stop" message to the output
         * queue.
         */
        @Override
        public void sendLastAndStop()
        {
            if (!stopped) {
                if (selector == null) {
                    LOG.error("Cannot queue stop message after engine has" +
                              " been destroyed");
                } else {
                    queueStopMessage();
                }
            }
        }

        /**
         * Start processing output.
         */
        void startProcessing()
        {
            // make sure the channel is non-blocking
            if (channel.isOpen() && channel instanceof SelectableChannel) {
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
            while (channel.isOpen()) {
                ByteBuffer buf;
                synchronized (outputQueue) {
                    if (outputQueue.size() == 0) {
                        LOG.error("Cannot transmit; no records found");
                        break;
                    }
                    buf = outputQueue.remove(0);
                }

                final int payLen = getRecordLength(buf);

                if (stopped) {
                    if (!isStopMessage(buf, payLen)) {
                        LOG.error("Channel " + name + " saw " + payLen +
                                  "-byte payload after stop");
                        bytesLeft = 0;
                        break;
                    }
                } else if (!channel.isOpen()) {
                    LOG.error("Channel " + name + " saw " + payLen +
                              "-byte payload after close");
                    break;
                } else {
                    buf.position(0);
                    buf.limit(payLen);

                    int numWritten = 0;
                    while (numWritten < payLen) {
                        if (!channel.isOpen()) {
                            LOG.error("Channel " + name +
                                      " closed while trying to write " +
                                      (payLen - numWritten) + " of " +
                                      payLen + " bytes");
                            break;
                        }

                        try {
                            int bytes = channel.write(buf);
                            brokenPipe = false;
                            if (bytes < 0) {
                                LOG.error("Channel " + name + " write of " +
                                          payLen + " bytes returned " + bytes);
                                break;
                            }
                            numWritten += bytes;
                        } catch (IOException ioe) {
                            if (ioe.getMessage() != null &&
                                ioe.getMessage().endsWith("Broken pipe"))
                            {
                                if (!brokenPipe) {
                                    brokenPipe = true;
                                    LOG.error("Channel " + name + " failed",
                                              ioe);
                                }
                            } else {
                                LOG.error("Channel " + name + " write failed",
                                          ioe);
                            }
                            break;
                        }
                    }

                    bytesLeft -= numWritten;

                    chanSent++;
                    numSent++;
                    totalSent++;
                }

                if (isStopMessage(buf, payLen)) {
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
        @Override
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

    /**
     * Simple output channel which sends a 4 byte payload stop message.
     */
    class PayloadStopChannel
        extends SimpleOutputChannel
    {
        /** Number of bytes in 'stop' message'. */
        private static final int STOP_MESSAGE_SIZE = 4;

        /**
         * Create a simple output channel which uses payload stop messages.
         *
         * @param parent parent output engine
         * @param name channel name
         * @param channel output channel
         * @param bufferMgr byte buffer manager
         */
        PayloadStopChannel(SimpleOutputEngine parent, String name,
                           WritableByteChannel channel,
                           IByteBufferCache bufferMgr, int maxDepth)
        {
            super(parent, name, channel, bufferMgr, maxDepth);
        }

        @Override
        int getRecordLength(ByteBuffer buf)
        {
            if (buf.limit() < 4) {
                return 0;
            }

            return buf.getInt(0);
        }

        /**
         * Is this a stop message?
         */
        @Override
        boolean isStopMessage(ByteBuffer buf, int payLen)
        {
            return payLen == STOP_MESSAGE_SIZE;
        }

        /**
         * Add a "stop" message to the output queue.
         */
        @Override
        void queueStopMessage()
        {
            ByteBuffer stopMessage = ByteBuffer.allocate(STOP_MESSAGE_SIZE);
            stopMessage.putInt(0, STOP_MESSAGE_SIZE);
            receiveByteBuffer(stopMessage);
        }
    }
}
