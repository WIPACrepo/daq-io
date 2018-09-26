package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Timer;
import java.util.TimerTask;

/**
 * An output engine utilizing blocking output, application output
 * buffering and executing on the client thread.
 *
 * This class is implemented to be substituted for a SimpleOutputEngine
 * within a DAQComponent, notably it must implement the (unenforced)
 * contract of the mbean interface defined in DAQComponent:
 * <PRE>
 *    public long[] getDepth();
 *    public long getRecordsSent();
 * </PRE>
 *
 * NOTES:
 * Users of this engine should investigate their use cases carefully for
 * starvation effects of the buffer as well as threading concerns brought
 * out by using the caller's thread for blocking output. An optional
 * time-based autoflush feature is available to control the liveliness
 * of the data channel.
 *
 * In certain conditions, the best performing output mechanism is
 * blocking. This engine functions as if it were dispatching directly
 * to a java.util.OutputStream but is implemented in terms of
 * a java.nio.channels.WritableByteChannel to conform to the DAQComponent
 * framework.
 */
public class BlockingOutputEngine implements DAQComponentOutputProcess
{
    private static final Logger logger =
            Logger.getLogger(BlockingOutputEngine.class);

    /** The output delegate, populated on connect. */
    private BufferedOutputChannel channel;

    /** Auto-flush settings. */
    private final boolean autoflush;
    private final long autoflushPeriod;

    /**
     * Counters of messages for the most recent connection as well as
     * total for all connections.
     *
     * These are managed awkwardly but should duplicate what is found
     * in SimpleOutputEngine;
     */
    private long lastSent;    // saved from last channel
    private long priorSent;   // saved all last channels


    /**  Size of buffer. */
    private final int bufferSize;

    /** defined states duplicated from SimpleOutputEngine*/
    private enum State { STOPPED, RUNNING, DESTROYED }

    private State state = State.STOPPED;


    /**
     * Constructor.
     * @param bufferSize The number of output bytes to buffer.
     */
    public BlockingOutputEngine(final int bufferSize)
    {
       this(bufferSize, false, Long.MAX_VALUE);
    }

    /**
     * Constructor.
     * @param bufferSize The number of output bytes to buffer.
     * @param autoflush  If <code>true</code>, channel will be flushed
     *                   automatically at the given period.
     * @param autoflushPeriod The period in milliseconds for automatic
     *                        channel flushing.
     */
    public BlockingOutputEngine(final int bufferSize, final boolean autoflush,
                                final long autoflushPeriod)
    {
        this.bufferSize = bufferSize;
        this.autoflush = autoflush;
        this.autoflushPeriod = autoflushPeriod;
    }

    @Override
    public ChannelRequirements getChannelRequirement()
    {
        return ChannelRequirements.BLOCKING;
    }

    @Override
    public QueuedOutputChannel addDataChannel(final WritableByteChannel channel,
                                              final IByteBufferCache bufMgr)
    {
        if (state != State.STOPPED) {
            throw new Error("Engine should be stopped, not " +
                    getPresentState());
        }

        if(this.channel != null)
        {
            throw new Error("Multiple connections not supported");
        }

        this.channel = new BufferedOutputChannel(this, bufMgr, channel,
                bufferSize);

        if(autoflush)
        {
            this.channel.enableAutoFlush(autoflushPeriod, autoflushPeriod);
        }

        return this.channel;
    }

    @Override
    public QueuedOutputChannel connect(final IByteBufferCache bufMgr,
                                       final WritableByteChannel channel,
                                       final int srcId)
            throws IOException
    {
        return this.addDataChannel(channel, bufMgr);
    }

    @Override
    public void disconnect() throws IOException
    {
        if (state == State.DESTROYED) {
            throw new Error("Engine has been destroyed");
        }

        sendLastAndStop();
    }

    @Override
    public void destroyProcessor()
    {
        sendLastAndStop();
        state = State.DESTROYED;
    }

    @Override
    public void forcedStopProcessing()
    {
        sendLastAndStop();
    }

    @Override
    public int getNumberOfChannels()
    {
        return channel != null ? 1 : 0;
    }

    @Override
    public String getPresentState()
    {
        return state.name();
    }

    @Override
    public boolean isDestroyed()
    {
        return state == State.DESTROYED;
    }

    @Override
    public boolean isRunning()
    {
        return state == State.RUNNING;
    }

    @Override
    public boolean isStopped()
    {
        return state == State.STOPPED;
    }

    @Override
    public void registerComponentObserver(final DAQComponentObserver observer)
    {
        throw new Error("Not Implemented");
    }

    @Override
    public void start()
    {
        //noop
    }

    @Override
    public void startProcessing()
    {
        if (state != State.STOPPED) {
            throw new Error("Engine should be stopped, not " +
                    getPresentState());
        }

        if(channel != null)
        {
            state = State.RUNNING;
        }
        else
        {
            state = State.STOPPED;
        }
    }

    @Override
    public long getRecordsSent()
    {
        return channel==null ? lastSent : channel.delegate.numSent();
    }

    @Override
    public long getTotalRecordsSent()
    {
        // Note: we only accumulate counts in priorSent and lastSent when
        //       we stop and close the channel
        return priorSent +
                (channel==null ? 0 : channel.delegate.numSent());
    }

    @Override
    public boolean isConnected()
    {
        return (channel != null);
    }

    @Override
    public void sendLastAndStop()
    {
        if(channel != null)
        {
            // The channel will manage stopping the
            // engine via the removeChannel() callback.
            channel.sendLastAndStop();
        }
        else
        {
            state = State.STOPPED;
        }
    }

    @Override
    public OutputChannel getChannel()
    {
        return channel;
    }

    /**
     * Required for mbean compatibility with SimpleOutputEngine.
     * @return
     */
    public long[] getDepth()
    {
        if(channel != null)
        {
            return new long[] { channel.delegate.bufferedMessages() };
        }
        else
        {
            return new long[0];
        }
    }


    /**
     * Handles a callback from the enclosed output channel to
     * effect an engine stop whenever the output channel is stopped.
     */
    private void removeChannel(final BufferedOutputChannel caller)
    {
        if(caller != channel)
        {
            throw new Error("Rogue channel");
        }

        channel.cancelAutoFlush();

        // counter accounting
        lastSent  = channel.numSent();
        priorSent += lastSent;
        channel = null;


        state = State.STOPPED;
    }


    /**
     * An adapter around a BufferedWritableChannel to realize the interface
     * defined by the DAQComponent framework.
     */
    static class BufferedOutputChannel implements QueuedOutputChannel
    {
        /** enclosing engine */
        final BlockingOutputEngine parent;

        /** sink channel, must be synchronized for autoflush thread */
        private final BufferedWritableChannel delegate;

        /** flag for stopped state */
        volatile boolean isStopped;

        /** Size of zero-filled message sent on channel stop. */
        private final int STOP_MESSAGE_SIZE = 4;

        /* Holds auto-flush components. */
        private final AutoFlush autoFlush;

        BufferedOutputChannel( final BlockingOutputEngine parent,
                               final IByteBufferCache bufferCache,
                               final WritableByteChannel delegate,
                               final int size)
        {
            this.parent = parent;
            this.delegate = new BufferedWritableChannel(bufferCache,
                    delegate, size);
            this.autoFlush = new AutoFlush();
        }

        @Override
        public void flushOutQueue()
        {
            try
            {
                synchronized (delegate)
                {
                    delegate.flush();
                }
            }
            catch (IOException ioe)
            {
                logger.error(ioe);
            }
        }

        @Override
        public boolean isOutputQueued()
        {
            synchronized (delegate)
            {
                return delegate.bufferedBytes() > 0;
            }
        }

        @Override
        public void receiveByteBuffer(final ByteBuffer buf)
        {
            try
            {
                synchronized (delegate)
                {
                    delegate.write(buf);
                }
            }
            catch (IOException ioe)
            {
                logger.error(ioe);
            }
        }

        @Override
        public void sendLastAndStop()
        {
            // Note: To exactly replicate SimpleOutputEngine we need to:
            //       1. Send a 4-byte zero-filled message
            //       2. Count the message as a "sent message"
            //       3. Do not perform accounting for the message using
            //          the buffer cache
            //       4. Cause the state of our enclosing BlockingOutputEngine
            //          to be STOPPED.
            //
            //       #4 is realized via a callback to the enclosing engine
            //       which is responsible for state management.

            if(!isStopped)
            {
                try
                {
                    ByteBuffer stopMessage = ByteBuffer.allocate(STOP_MESSAGE_SIZE);
                    stopMessage.putInt(0, STOP_MESSAGE_SIZE);

                    synchronized (delegate)
                    {
                        delegate.writeEndMessage(stopMessage);
                        delegate.close();
                    }
                    isStopped = true;

                    parent.removeChannel(this);
                }
                catch (IOException ioe)
                {
                    logger.error(ioe);
                }
            }
        }

        @Override
        public void registerComponentObserver(
                final DAQComponentObserver compObserver,
                final String notificationID)
        {
            throw new Error("Not Implemented");
        }

        /**
         * @return The number of messages sent.
         */
        public long numSent()
        {
            synchronized (delegate)
            {
                return delegate.numSent();
            }
        }

        public void enableAutoFlush(long delay, long period)
        {
            autoFlush.schedule(delay, period);
        }

        public void cancelAutoFlush()
        {
            autoFlush.cancel();
        }

        /**
         * Provides a time-based channel flush of the enclosing
         * instance.
         */
        private class AutoFlush extends TimerTask
        {
            private Timer timer;

            @Override
            public void run()
            {
                synchronized (this)
                {
                    BufferedOutputChannel.this.flushOutQueue();
                }
            }

            private void schedule(long delay, long period)
            {
                synchronized (this)
                {
                    if(timer != null)
                    {
                        throw new Error("Already scheduled");
                    }

                    timer = new Timer(true);
                    timer.schedule(this, delay, period);
                }
            }


            @Override
            public boolean cancel()
            {
                synchronized (this)
                {
                    boolean ret = super.cancel();
                    if(timer != null)
                    {
                        timer.cancel();
                        timer = null;
                    }
                    return ret;
                }
            }
        }

    }




}
