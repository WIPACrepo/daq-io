/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java,v 1.3 2005/04/06 01:37:42 mcp Exp $
 *
 * Date: March 24 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.common.DAQComponentObserver;

import java.nio.channels.WritableByteChannel;

/**
 * This represents the engine for the transmit channels.
 *
 * @version $Id: DAQComponentOutputProcess.java,v 1.3 2005/04/06 01:37:42 mcp Exp $
 * @author mcp
 */
public interface DAQComponentOutputProcess extends DAQComponentIOProcess {

    public void sendLastAndStop();
    
    public PayloadTransmitChannel addDataChannel(WritableByteChannel channel, IByteBufferCache bufMgr);

}
