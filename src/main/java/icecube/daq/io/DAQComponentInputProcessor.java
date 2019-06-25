/**
 * DAQComponentInputProcessor
 * Date: Nov 13, 2006 4:45:35 PM
 *
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

/**
 * @author mcp
 * @version $Id: DAQComponentInputProcessor.java 2629 2008-02-11 05:48:36Z dglo $
 */
public interface DAQComponentInputProcessor extends DAQComponentIOProcess {

    int getServerPort();

    boolean isDisposing();

    void startDisposing();

    void startServer(IByteBufferCache cache)
        throws IOException;
}
