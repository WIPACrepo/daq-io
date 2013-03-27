/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 14365 2013-03-27 16:05:22Z dglo $
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
 * @version $Id: DAQComponentOutputProcess.java 14365 2013-03-27 16:05:22Z dglo $
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

    void forcedStopProcessing();

    long[] getRecordsSent();

    boolean isConnected();

    void sendLastAndStop();

}
