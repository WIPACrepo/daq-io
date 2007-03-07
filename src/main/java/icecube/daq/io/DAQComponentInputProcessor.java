/**
 * DAQComponentInputProcessor
 * Date: Nov 13, 2006 4:45:35 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.common.DAQComponentObserver;

import java.io.IOException;

import java.nio.channels.ReadableByteChannel;

/**
 * @author mcp
 * @version $Id: DAQComponentInputProcessor, v 1.1, Nov 13, 2006 4:45:35 PM artur $
 */
public interface DAQComponentInputProcessor extends DAQComponentIOProcess {

    public int getServerPort();

    public boolean isDisposing();

    public void startDisposing();

    public void startServer(IByteBufferCache cache)
        throws IOException;
}
