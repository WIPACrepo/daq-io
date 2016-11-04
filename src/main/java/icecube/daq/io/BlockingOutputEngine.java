package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

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
 * out by using the caller's thread for blocking output.
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


    public BlockingOutputEngine(final int bufferSize)
    {
        this.bufferSize = bufferSize;
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

        BufferedWritableChannel bufferingWrapper =
                new BufferedWritableChannel(bufMgr, channel, bufferSize);

        this.channel = new BufferedOutputChannel(bufferingWrapper);

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
        throw new Error("Not Imlemented");
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
            channel.sendLastAndStop();
            lastSent  = channel.delegate.numSent();
            priorSent += lastSent;
            channel = null;
        }
        state = State.STOPPED;
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
     * An adapter around a BufferedWritableChannel to realize the interface
     * defined by the DAQComponent framework.
     */
    static class BufferedOutputChannel implements QueuedOutputChannel
    {
        private final BufferedWritableChannel delegate;

        BufferedOutputChannel(final BufferedWritableChannel channel)
        {
            this.delegate = channel;
        }

        @Override
        public void flushOutQueue()
        {
            try
            {
                delegate.flush();
            }
            catch (IOException ioe)
            {
                logger.error(ioe);
            }
        }

        @Override
        public boolean isOutputQueued()
        {
            return delegate.bufferedBytes() > 0;
        }

        @Override
        public void receiveByteBuffer(final ByteBuffer buf)
        {
            try
            {
                delegate.write(buf);
            }
            catch (IOException ioe)
            {
                logger.error(ioe);
            }
        }

        @Override
        public void sendLastAndStop()
        {
            try
            {
                delegate.flush();
                delegate.close();
            }
            catch (IOException ioe)
            {
                logger.error(ioe);
            }
        }

        @Override
        public void registerComponentObserver(
                final DAQComponentObserver compObserver,
                final String notificationID)
        {
            throw new Error("Not Implemented");
        }

    }


}
