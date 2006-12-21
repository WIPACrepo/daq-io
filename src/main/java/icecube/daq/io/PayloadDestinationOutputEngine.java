/*
 * class: PayloadDestinationOutputEngine
 *
 * Version $Id: PayloadDestinationOutputEngine.java,v 1.5 2005/11/30 00:48:00 artur Exp $
 *
 * Date: May 23 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IPayloadDestinationCollectionController;
import icecube.daq.payload.IPayloadDestinationCollection;
import icecube.daq.payload.PayloadDestinationCollection;
import icecube.daq.payload.ByteBufferPayloadDestination;
import icecube.daq.payload.SinkPayloadDestination;

import java.nio.channels.WritableByteChannel;

/**
 * This class provides two options :
 * 1. Dispose the buffers
 * 2. Push buffers in the transmit channel
 *
 * @author mcp
 * @version $Id: PayloadDestinationOutputEngine.java,v 1.5 2005/11/30 00:48:00 artur Exp $
 */
public class PayloadDestinationOutputEngine extends SourceIdPayloadOutputEngine
        implements IPayloadDestinationCollectionController {

    /**
     * Payload destination collection.
     */
    private IPayloadDestinationCollection payloadDestinationCollection;

    /**
     * Set the default payload destination type.
     */
    private int payloadDestinationType = IPayloadDestinationCollectionController.BYTE_BUFFER_PAYLOAD_DESTINATION;

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public PayloadDestinationOutputEngine(
                                          String type,
                                          int id,
                                          String fcn) {
        // parent constructor wants same args
        super(type, id, fcn);
        payloadDestinationCollection = new PayloadDestinationCollection();
        payloadDestinationCollection.registerController(this);
    }


    public PayloadTransmitChannel addDataChannel(WritableByteChannel channel, ISourceID sourceID) {
        PayloadTransmitChannel eng = super.addDataChannel(channel, sourceID);

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
