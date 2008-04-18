/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 2950 2008-04-18 23:20:55Z dglo $
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
 * @version $Id: DAQComponentOutputProcess.java 2950 2008-04-18 23:20:55Z dglo $
 * @author mcp
 */
public interface DAQComponentOutputProcess
    extends DAQComponentIOProcess, DAQOutputChannelManager
{

    QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                       IByteBufferCache bufMgr);

    QueuedOutputChannel connect(IByteBufferCache bufMgr,
                                WritableByteChannel chan, int srcId)
        throws IOException;

    void disconnect()
        throws IOException;

    long[] getRecordsSent();

    boolean isConnected();

    void sendLastAndStop();

}
