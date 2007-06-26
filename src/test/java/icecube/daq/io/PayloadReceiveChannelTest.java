package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.Semaphore;

import icecube.daq.io.PayloadReceiveChannel;

import icecube.daq.io.test.MockAppender;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.VitreousBufferCache;

import java.io.IOException;

import java.nio.channels.Pipe;
import java.nio.channels.Selector;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

public class PayloadReceiveChannelTest
    extends TestCase
{
    private static final int BUFFER_BLEN = 5000;

    private static Level logLevel = Level.INFO;

    public PayloadReceiveChannelTest(String name)
    {
        super(name);
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(new MockAppender(logLevel));
    }

    public void testConstructor()
        throws IOException
    {
        Selector sel = Selector.open();

        Pipe testPipe = Pipe.open();

        Pipe.SinkChannel sinkChan = testPipe.sink();
        sinkChan.configureBlocking(false);

        Pipe.SourceChannel srcChan = testPipe.source();

        IByteBufferCache cacheMgr = new VitreousBufferCache();

        Semaphore inputSem = new Semaphore(0);

        PayloadReceiveChannel rcvChan =
            new PayloadReceiveChannel("basic", sel, srcChan, cacheMgr,
                                      inputSem);

        assertFalse("Expected channel to be non-blocking",
                    srcChan.isBlocking());
    }

    public static Test suite()
    {
        return new TestSuite(PayloadReceiveChannelTest.class);
    }

    public static void main(String[] args)
    {
        TestRunner.run(suite());
    }
}
