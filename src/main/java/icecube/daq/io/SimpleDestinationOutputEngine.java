/*
 * class: SimpleDestinationOutputEngine
 *
 * Version $Id: SimpleDestinationOutputEngine.java 2629 2008-02-11 05:48:36Z dglo $
 *
 * Date: Feb 22 2008
 *
 * (c) 2008 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadDestinationCollection;
import icecube.daq.payload.IPayloadDestinationCollectionController;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IWriteablePayload;
import icecube.daq.payload.PayloadDestination;
import icecube.daq.payload.PayloadDestinationCollection;
import icecube.daq.payload.impl.SourceID;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;

/**
 * This object is a PayloadDestination that is able to write a Payload to
 * a new ByteBuffer which is acquired from an IByteBufferCache and pass this
 * newly created and written-to ByteBuffer (which now contains the contents of
 * a Payload) to a QueuedOutputChannel.
 *
 * @author dwharton
 */
class ByteBufferPayloadDestination
    extends PayloadDestination
{
    /**
     * The IByteBufferCache from which new ByteBuffer's are acquired to
     * write Payloads to.
     */
    private IByteBufferCache cache;

    /**
     * The QueuedOutputChannel object which is to receive the ByteBuffer that
     * has been allocated and written into.
     */
    protected QueuedOutputChannel outChan;

    /**
     * Constructor.
     * @param receiver QueuedOutputChannel the object which will receive the
     *  ByteBuffer which has been created by subsequent calls to the
     *  PayloadDestination.
     * @param cache the IByteBufferCache which is used to acquire byte-buffers
     *  to write to.
     */
    public ByteBufferPayloadDestination(QueuedOutputChannel receiver,
                                        IByteBufferCache cache) {
        if (cache == null) {
            throw new Error("Buffer cache is null");
        }

        outChan = receiver;
        this.cache = cache;
    }

    /**
     * This methods proxies the call to write Payload to allow the whole
     * payload to be passed to the payload destination to allow it to
     * be invoke the write method itself, or to pass the payload by refernce
     * to the target.
     *
     * @param tPayload Payload to which to write to this destination
     * @return the length in bytes which was written to the ByteBuffer.
     *
     * @throws IOException if an error occurs during the process
     */
    public int writePayload(IWriteablePayload tPayload) throws IOException {
        return writePayload(false,tPayload);
    }

    /**
     * This methods proxies the call to write Payload to allow the whole
     * payload to be passed to the payload destination to allow it to
     * be invoke the write method itself, or to pass the payload by refernce
     * to the target.
     *
     * @param bWriteLoaded boolean to indicate if the loaded vs buffered
     *  payload should be written.
     * @param tPayload Payload to which to write to this destination
     * @return the length in bytes which was written to the ByteBuffer.
     *
     * @throws IOException if an error occurs during the process
     */
    public int writePayload(boolean bWriteLoaded, IWriteablePayload tPayload)
        throws IOException
    {
        if (outChan == null) {
            throw new IOException("This PayloadDestination is not valid");
        }

        int iPayloadLength = tPayload.length();
        ByteBuffer tBuffer = cache.acquireBuffer(iPayloadLength);
        if (tBuffer == null) {
            throw new RuntimeException("Could not acquire buffer");
        }

        tBuffer.clear();
        int iWrittenLength = tPayload.writePayload(bWriteLoaded,0,tBuffer);
        if (iPayloadLength != iWrittenLength) {
            throw new RuntimeException("Problem when acquireBuffer" +
                                       " iPayloadLength: " + iPayloadLength +
                                       " iWrittenLength: " + iWrittenLength +
                                       " tBuffer: " + tBuffer.capacity());
        }
        //-this makes sure that the buffer position, capacity, etc is set.
        tBuffer.clear();
        tBuffer.position(0);
        tBuffer.limit(iWrittenLength);
        //
        notifyReceiver(tBuffer);
        return iWrittenLength;

    }

    /**
     * Notifies the installed receiver of the new byte-buffer which has been
     * created.
     * @param tBuffer the new ByteBuffer which has been created.
     */
    public void notifyReceiver(ByteBuffer tBuffer) {
        outChan.receiveByteBuffer(tBuffer);
    }

    /**
     * Optionally receive the ByteBuffer back for reuse.
     * @param  tBuffer ByteBuffer the buffer which can be reused.
     */
    public void recycleByteBuffer(ByteBuffer tBuffer) {
        cache.returnBuffer(tBuffer);
    }

    /**
     * Closes this channel.
     *
     * <p> After a channel is closed, any further attempt to invoke I/O
     * operations upon it will cause a
     * {@link java.nio.channels.ClosedChannelException} to be thrown.
     *
     * <p> If this channel is already closed then invoking this method has no
     * effect.
     *
     * <p> This method may be invoked at any time.  If some other thread has
     * already invoked it, however, then another invocation will block until
     * the first invocation is complete, after which it will return without
     * effect. </p>
     *
     * @throws  IOException  If an I/O error occurs
     */
    public void close() throws IOException {
        outChan.flushOutQueue();
        outChan = null;
    }
}

public class SimpleDestinationOutputEngine
    extends SimpleOutputEngine
    implements DAQSourceIdOutputProcess,
               IPayloadDestinationCollectionController
{
    /** Byte buffer manager. */
    private IByteBufferCache bufMgr;
    /** Mapping of ISourceID to destination. */
    private HashMap idRegistry = new HashMap();
    /** Total number of messages sent via the sendPayload() method. */
    private long messagesSent = 0;

    /** Payload destination collection. */
    private IPayloadDestinationCollection payloadDestinationCollection;

    /**
     * Create a destination output engine.
     *
     * @param type engine type
     * @param id engine ID
     * @param fcn engine function
     */
    public SimpleDestinationOutputEngine(String type, int id, String fcn)
    {
        super(type, id, fcn);
        payloadDestinationCollection = new PayloadDestinationCollection();
        payloadDestinationCollection.registerController(this);
    }

    /**
     * Add an output channel.
     *
     * @param channel output channel
     * @param sourceID source ID for the output channel
     */
    public QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                              ISourceID sourceID)
    {
        // ask payloadOutputEngine to make us a payloadTransmitEngine
        QueuedOutputChannel eng = super.addDataChannel(channel, bufMgr);
        // register it locally so that we can find it when we need it
        idRegistry.put(sourceID, eng);

        // add a PayloadDestination to the Collection
        ByteBufferPayloadDestination dest =
            new ByteBufferPayloadDestination(eng, bufMgr);
        payloadDestinationCollection.addPayloadDestination(sourceID, dest);

        return eng;
    }

    /**
     * Callback method that indicates all PayloadDestinations have been closed.
     */
    public void allPayloadDestinationsClosed()
    {
        sendLastAndStop();
    }

    /**
     * Connect this output channel to the engine using the specified source ID
     *
     * @param bufCache byte buffer manager (ignored)
     * @param chan output channel
     * @param srcId remote source ID
     */
    public QueuedOutputChannel connect(IByteBufferCache bufCache,
                                       WritableByteChannel chan, int srcId)
    {
        return addDataChannel(chan, new SourceID(srcId));
    }

    /**
     * Get reference to buffer manager
     */
    public IByteBufferCache getBufferManager()
    {
        return bufMgr;
    }

    /**
     * Total number of messages sent via sendPayload()
     *
     * @return number of messages sent
     */
    public long getMessagesSent()
    {
        return messagesSent;
    }

    /**
     * Get the PayloadDestinationCollection.
     *
     * @return the PayloadDestinationCollection
     */
    public IPayloadDestinationCollection getPayloadDestinationCollection()
    {
        return payloadDestinationCollection;
    }

    /**
     * Find the output channel associated with the specified source ID
     *
     * @return <tt>null</tt> if no channel is associated with the source ID
     */
    public QueuedOutputChannel lookUpEngineBySourceID(ISourceID id)
    {
        if (!idRegistry.containsKey(id)) {
            return null;
        }

        return (QueuedOutputChannel) idRegistry.get(id);
    }

    /**
     * Callback method that indicates that the PayloadDestination associated
     * with this SourceId has been closed by the user.
     *
     * @param sourceId SourceId of closed PayloadDestination
     */
    public void payloadDestinationClosed(ISourceID sourceId)
    {
        // do nothing
    }

    /**
     * Keep reference to buffer manager so that we can return buffers
     * after transmits  Note: this interface is needed because the buffer
     * manager is not available to the parent class when creating this class
     */
    public void registerBufferManager(IByteBufferCache manager)
    {
        bufMgr = manager;
    }

    /**
     * Sent the payload to the specified source ID.
     *
     * @param id target source ID
     * @param payload data to be sent
     */
    public void sendPayload(ISourceID id, ByteBuffer payload)
    {
        if (!idRegistry.containsKey(id)) {
            throw new RuntimeException("SourceID " + id + "not registered");
        }

        QueuedOutputChannel eng = (QueuedOutputChannel) idRegistry.get(id);
        eng.receiveByteBuffer(payload);
        messagesSent++;
    }
}
