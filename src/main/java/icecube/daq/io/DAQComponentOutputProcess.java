/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 15428 2015-02-20 19:01:46Z dglo $
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
 * @version $Id: DAQComponentOutputProcess.java 15428 2015-02-20 19:01:46Z dglo $
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

    long getRecordsSent();

    long getTotalRecordsSent();

    boolean isConnected();

    void sendLastAndStop();

}
