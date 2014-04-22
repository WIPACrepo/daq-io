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

import icecube.daq.payload.ByteBufferPayloadDestination;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadDestinationCollection;
import icecube.daq.payload.IPayloadDestinationCollectionController;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.PayloadDestinationCollection;
import icecube.daq.payload.impl.SourceID;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;

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
