/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 17113 2018-09-26 09:40:57Z dglo $
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
 * @version $Id: DAQComponentOutputProcess.java 17113 2018-09-26 09:40:57Z dglo $
 * @author mcp
 */
public interface DAQComponentOutputProcess
    extends DAQComponentIOProcess, DAQOutputChannelManager
{

    /**
     * Enumerate available channel options.
     */
    public enum ChannelRequirements
    {
        BLOCKING,
        NON_BLOCKING
    }

    /**
     * Advertises the required configuration of the channel
     * provided to the connect() and addDataChannel() methods.
     * By default, a non-blocking channel will be requested.
     */
    default public ChannelRequirements getChannelRequirement()
    {
        return ChannelRequirements.NON_BLOCKING;
    }

    QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                       IByteBufferCache bufMgr);

    QueuedOutputChannel connect(IByteBufferCache bufMgr,
                                WritableByteChannel chan, int srcId)
        throws IOException;

    void disconnect()
        throws IOException;

    long getRecordsSent();

    long getTotalRecordsSent();

    boolean isConnected();

    void sendLastAndStop();

}
