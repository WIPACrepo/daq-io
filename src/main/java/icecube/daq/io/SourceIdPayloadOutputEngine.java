/*
 * class: SystemTestPayloadOutputEngine
 *
 * Version $Id: SourceIdPayloadOutputEngine.java 2268 2007-11-09 16:49:57Z dglo $
 *
 * Date: May 23 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.impl.SourceID4B;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class ...does what?
 *
 * @author mcp
 * @version $Id: SourceIdPayloadOutputEngine.java 2268 2007-11-09 16:49:57Z dglo $
 */
public class SourceIdPayloadOutputEngine extends PayloadOutputEngine {

    // component name of creator
    private String componentType;
    // component ID of creator
    private int componentID;
    // component fcn
    private String componentFcn;
    private IByteBufferCache bufMgr;
    private HashMap idRegistry = new HashMap();
    private long messagesSent = 0;
    private Log log = LogFactory.getLog(SourceIdPayloadOutputEngine.class);

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public SourceIdPayloadOutputEngine(String type,
                                       int id,
                                       String fcn) {
        // parent constructor wants same args
        super(type, id, fcn);
        componentType = type;
        componentID = id;
        componentFcn = fcn;
    }

    // instance member method (alphabetic)
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

    public PayloadTransmitChannel connect(IByteBufferCache bufCache, WritableByteChannel chan,
                                          int srcId) throws IOException {
        return addDataChannel(chan, new SourceID4B(srcId));
    }

    public PayloadTransmitChannel addDataChannel(WritableByteChannel channel, ISourceID sourceID){
        // ask payloadOutputEngine to make us a payloadTransmitEngine
        PayloadTransmitChannel eng = super.addDataChannel(channel, bufMgr);
        // register it locally so that we can find it when we need it
        idRegistry.put(new Integer(sourceID.getSourceID()), eng);
        return eng;
    }

    public PayloadTransmitChannel lookUpEngineBySourceID(ISourceID id) {
        Integer realID = new Integer(id.getSourceID());
        if (idRegistry.containsKey(realID)) {
            return (PayloadTransmitChannel) idRegistry.get(realID);
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
}
