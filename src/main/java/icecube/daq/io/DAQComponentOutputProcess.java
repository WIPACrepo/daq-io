/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 2125 2007-10-12 18:27:05Z ksb $
 *
 * Date: March 24 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.common.DAQComponentObserver;

import java.io.IOException;

import java.nio.channels.WritableByteChannel;

/**
 * This represents the engine for the transmit channels.
 *
 * @version $Id: DAQComponentOutputProcess.java 2125 2007-10-12 18:27:05Z ksb $
 * @author mcp
 */
public interface DAQComponentOutputProcess extends DAQComponentIOProcess {

    public PayloadTransmitChannel addDataChannel(WritableByteChannel channel, IByteBufferCache bufMgr);

    public PayloadTransmitChannel connect(IByteBufferCache bufMgr, WritableByteChannel chan, int srcId)
        throws IOException;

    public void disconnect()
        throws IOException;

    public boolean isConnected();

    public void sendLastAndStop();
    
}
