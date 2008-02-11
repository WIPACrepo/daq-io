package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

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
public class SimpleOutputChannel
    implements OutputChannel, Runnable
{
    private static final Log LOG = LogFactory.getLog(SimpleOutputChannel.class);

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
    SimpleOutputChannel(IOChannelParent parent, String name,
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
    void sendLastAndStop() {
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
