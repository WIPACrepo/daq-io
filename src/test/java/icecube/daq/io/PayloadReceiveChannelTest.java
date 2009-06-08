package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.Semaphore;

import icecube.daq.io.test.LoggingCase;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.VitreousBufferCache;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.Selector;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PayloadReceiveChannelTest
    extends LoggingCase
{
    private static final int BUFFER_BLEN = 5000;

    public PayloadReceiveChannelTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return new TestSuite(PayloadReceiveChannelTest.class);
    }

    public void testConstructor()
        throws IOException
    {
        Selector sel = Selector.open();

        Pipe testPipe = Pipe.open();

        Pipe.SinkChannel sinkChan = testPipe.sink();
        sinkChan.configureBlocking(false);

        Pipe.SourceChannel srcChan = testPipe.source();

        IByteBufferCache cacheMgr = new VitreousBufferCache("Ctor");

        Semaphore inputSem = new Semaphore(0);

        PayloadReceiveChannel rcvChan =
            new PayloadReceiveChannel("basic", sel, srcChan, cacheMgr,
                                      inputSem);

        assertFalse("Expected channel to be non-blocking",
                    srcChan.isBlocking());
    }

    public static void main(String[] args)
    {
        TestRunner.run(suite());
    }
}
