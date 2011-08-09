package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.Semaphore;

import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.ByteBuffer;
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

        IByteBufferCache cacheMgr = new MockBufferCache("Ctor");

        Semaphore inputSem = new Semaphore(0);

        PayloadReceiveChannel rcvChan =
            new PayloadReceiveChannel("basic", sel, srcChan, cacheMgr,
                                      inputSem);

        assertFalse("Expected channel to be non-blocking",
                    srcChan.isBlocking());
    }

    public void testMethods()
	throws Exception
    {
	ByteBuffer buf = ByteBuffer.allocate(10);
	Selector sel = Selector.open();

        Pipe testPipe = Pipe.open();

        Pipe.SinkChannel sinkChan = testPipe.sink();
        sinkChan.configureBlocking(false);

        Pipe.SourceChannel srcChan = testPipe.source();

        IByteBufferCache cacheMgr = new MockBufferCache("Ctor");

        Semaphore inputSem = new Semaphore(0);

        PayloadReceiveChannel rcvChan =
            new PayloadReceiveChannel("basic", sel, srcChan, cacheMgr,
                                      inputSem);

	rcvChan.setInputBufferSize( 1);
	rcvChan.returnBuffer( buf);
	rcvChan.exitIdle();
	rcvChan.exitRecvHeader();
	rcvChan.enterError();
	rcvChan.enterSplicerWait();
	rcvChan.notifyOnStop();
	rcvChan.startEngine();
	rcvChan.startDisposing();
	rcvChan.injectError();
	rcvChan.processTimer();
	rcvChan.transition( 1);
	rcvChan.doTransition( 1, 2);

	assertFalse("placeholder for code in SpliceablePayloadReceiveChannel", 
	    rcvChan.splicerAvailable());
	assertFalse(" returns if more payload can be handled ",
	    rcvChan.handleMorePayloads());
	assertNotNull("Current acquired bytes", 
	    rcvChan.getBufferCurrentAcquiredBytes());
	assertNotNull("current acquired buffers", 
	    rcvChan.getBufferCurrentAcquiredBuffers());
	assertNotNull("present state", rcvChan.presentState());
	assertNotNull("String returned", rcvChan.toString());
	assertEquals("get Number", 1, rcvChan.getNum());

	rcvChan.stopEngine();
	rcvChan.close();
	


	
    }

    public static void main(String[] args)
    {
        TestRunner.run(suite());
    }
}
