/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 2228 2007-11-02 16:02:54Z dglo $
 *
 * Date: March 24 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

import java.nio.channels.WritableByteChannel;

/**
 * This represents the engine for the transmit channels.
 *
 * @version $Id: DAQComponentOutputProcess.java 2228 2007-11-02 16:02:54Z dglo $
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
