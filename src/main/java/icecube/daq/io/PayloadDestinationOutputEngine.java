/*
 * class: PayloadDestinationOutputEngine
 *
 * Version $Id: PayloadDestinationOutputEngine.java 2852 2008-03-26 10:39:43Z dglo $
 *
 * Date: May 23 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.ByteBufferPayloadDestination;
import icecube.daq.payload.IPayloadDestinationCollection;
import icecube.daq.payload.IPayloadDestinationCollectionController;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.PayloadDestinationCollection;

import java.nio.channels.WritableByteChannel;

/**
 * This class pushes buffers into the transmit channels
 *
 * @author mcp
 * @version $Id: PayloadDestinationOutputEngine.java 2852 2008-03-26 10:39:43Z dglo $
 */
public class PayloadDestinationOutputEngine extends SourceIdPayloadOutputEngine
        implements IPayloadDestinationCollectionController {

    /**
     * Payload destination collection.
     */
    private IPayloadDestinationCollection payloadDestinationCollection;

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


    public OutputChannel addDataChannel(WritableByteChannel channel, ISourceID sourceID) {
        OutputChannel eng = super.addDataChannel(channel, sourceID);

        // add a PayloadDestination to the Collection
        payloadDestinationCollection.addPayloadDestination(sourceID, new ByteBufferPayloadDestination(eng, getBufferManager()));

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
}
