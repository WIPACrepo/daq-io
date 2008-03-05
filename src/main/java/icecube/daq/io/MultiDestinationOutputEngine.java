/*
 * class: MultiDestinationOutputEngine
 *
 * Version $Id: MultiDestinationOutputEngine.java 2629 2008-02-11 05:48:36Z dglo $
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
import icecube.daq.payload.SinkPayloadDestination;
import icecube.daq.payload.impl.SourceID4B;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;

public class MultiDestinationOutputEngine extends MultiOutputEngine
        implements IPayloadDestinationCollectionController {

    private IByteBufferCache bufMgr;
    private HashMap idRegistry = new HashMap();
    private long messagesSent = 0;

    /**
     * Payload destination collection.
     */
    private IPayloadDestinationCollection payloadDestinationCollection;

    /**
     * Set the default payload destination type.
     */
    private int payloadDestinationType =
        IPayloadDestinationCollectionController.BYTE_BUFFER_PAYLOAD_DESTINATION;

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public MultiDestinationOutputEngine(String type, int id, String fcn) {
        // parent constructor wants same args
        super(type, id, fcn);
        payloadDestinationCollection = new PayloadDestinationCollection();
        payloadDestinationCollection.registerController(this);
    }

    // keep reference to buffer manager so that we can return buffers
    // after transmits  Note: this interface is needed because the buffer
    // manager is created in the SystemTestPayloadInputEngine class and
    // is not available to the parent class when creating this class
    public void registerBufferManager(IByteBufferCache manager) {
        bufMgr = manager;
    }

    // get reference to buffer manager
    public IByteBufferCache getBufferManager() {
        return bufMgr;
    }

    public OutputChannel connect(IByteBufferCache bufCache, WritableByteChannel chan,
                                 int srcId) throws IOException {
        return addDataChannel(chan, new SourceID4B(srcId));
    }

    public OutputChannel addDataChannel(WritableByteChannel channel, ISourceID sourceID) {
        // ask payloadOutputEngine to make us a payloadTransmitEngine
        OutputChannel eng = super.addDataChannel(channel, bufMgr);
        // register it locally so that we can find it when we need it
        idRegistry.put(new Integer(sourceID.getSourceID()), eng);

        // add a PayloadDestination to the Collection
        switch (payloadDestinationType) {
            case IPayloadDestinationCollectionController.SINK_PAYLOAD_DESTINATION:
                payloadDestinationCollection.addPayloadDestination(sourceID, new SinkPayloadDestination(eng));
                break;
            case IPayloadDestinationCollectionController.BYTE_BUFFER_PAYLOAD_DESTINATION:
                payloadDestinationCollection.addPayloadDestination(sourceID, new ByteBufferPayloadDestination(eng, getBufferManager()));
                break;
            default:
                payloadDestinationCollection.addPayloadDestination(sourceID, new ByteBufferPayloadDestination(eng, getBufferManager()));
                break;
        }

        return eng;
    }

    public OutputChannel lookUpEngineBySourceID(ISourceID id) {
        Integer realID = new Integer(id.getSourceID());
        if (idRegistry.containsKey(realID)) {
            return (OutputChannel) idRegistry.get(realID);
        } else {
            return null;
        }
    }

    public void sendPayload(ISourceID id, ByteBuffer payload){
        if (!idRegistry.containsKey(new Integer(id.getSourceID()))) {
            throw new RuntimeException("SourceID " + id.getSourceID() + "not registered");
        } else {
            PayloadTransmitChannel eng = (PayloadTransmitChannel) idRegistry.get(id);
            eng.receiveByteBuffer(payload);
            messagesSent++;
        }
    }

    public long getMessagesSent() {
        return messagesSent;
    }

    /**
     * Get the PayloadDestinationCollection.
     *
     * @return the PayloadDestinationCollection
     */
    public IPayloadDestinationCollection getPayloadDestinationCollection() {
        return payloadDestinationCollection;
    }

    /**
     * Callback method that indicates that the PayloadDestination associated with this SourceId has been closed by the
     * user.
     *
     * @param sourceId SourceId of closed PayloadDestination
     */
    public void payloadDestinationClosed(ISourceID sourceId) {
    }

    /**
     * Callback method that indicates all PayloadDestinations have been closed.
     */
    public void allPayloadDestinationsClosed() {
        sendLastAndStop();
    }

    /**
     * Set the type of PayloadDestination to use.
     *
     * @param type see PayloadDestinationRegistry
     */
    public void setPayloadDestinationType(int type) {
        payloadDestinationType = type;
    }

}
