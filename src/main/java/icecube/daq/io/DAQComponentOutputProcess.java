/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 2629 2008-02-11 05:48:36Z dglo $
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
 * @version $Id: DAQComponentOutputProcess.java 2629 2008-02-11 05:48:36Z dglo $
 * @author mcp
 */
public interface DAQComponentOutputProcess extends DAQComponentIOProcess {

    OutputChannel addDataChannel(WritableByteChannel channel, IByteBufferCache bufMgr);

    OutputChannel connect(IByteBufferCache bufMgr, WritableByteChannel chan, int srcId)
        throws IOException;

    void disconnect()
        throws IOException;

    boolean isConnected();

    void sendLastAndStop();

}
