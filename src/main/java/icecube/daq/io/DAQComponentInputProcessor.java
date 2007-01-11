/**
 * DAQComponentInputProcessor
 * Date: Nov 13, 2006 4:45:35 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.common.DAQComponentObserver;

import java.nio.channels.ReadableByteChannel;

/**
 * @author mcp
 * @version $Id: DAQComponentInputProcessor, v 1.1, Nov 13, 2006 4:45:35 PM artur $
 */
public interface DAQComponentInputProcessor extends DAQComponentIOProcess {

    public void startDisposing();

    public boolean isDisposing();

    public PayloadReceiveChannel addDataChannel(ReadableByteChannel channel, IByteBufferCache bufMgr);

}
