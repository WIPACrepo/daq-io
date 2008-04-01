/*
 * class: SimpleOutputEngineTest
 *
 * Version $Id: SimpleOutputEngineTest.java 2848 2008-03-25 22:03:17Z dglo $
 *
 * Date: May 19 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io.test;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.DAQComponentObserver;
import icecube.daq.io.ErrorState;
import icecube.daq.io.NormalState;
import icecube.daq.io.OutputChannel;
import icecube.daq.io.SimpleOutputEngine;
import icecube.daq.io.PayloadTransmitChannel;
import icecube.daq.io.test.MockObserver;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.VitreousBufferCache;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * This class defines the tests that any SimpleOutputEngine object should pass.
 *
 * @author mcp
 * @version $Id: SimpleOutputEngineTest.java 2848 2008-03-25 22:03:17Z dglo $
 */
public class SimpleOutputEngineTest
    extends LoggingCase
{
    // private static member data
    private static final int BUFFER_BLEN = 32000;
    private static final String SRC_NOTIFICATION_ID = "SourceID";
    private static final String SRC_ERROR_ID = "SourceErrorID";

    /**
     * The object being tested.
     */
    private SimpleOutputEngine engine;

    /**
     * Constructs and instance of this test.
     *
     * @param name the name of the test.
     */
    public SimpleOutputEngineTest(String name)
    {
        super(name);
    }

    private SocketChannel acceptChannel(Selector sel)
        throws IOException
    {
        SocketChannel chan = null;

        while (chan == null) {
            int numSel = sel.select(500);
            if (numSel == 0) {
                continue;
            }

            Iterator iter = sel.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey selKey = (SelectionKey) iter.next();
                iter.remove();

                if (!selKey.isAcceptable()) {
                    selKey.cancel();
                    continue;
                }

                ServerSocketChannel ssChan =
                    (ServerSocketChannel) selKey.channel();
                if (chan != null) {
                    System.err.println("Got multiple socket connections");
                    continue;
                }

                try {
                    chan = ssChan.accept();
                } catch (IOException ioe) {
                    System.err.println("Couldn't accept client socket");
                    ioe.printStackTrace();
                    chan = null;
                }
            }
        }

        return chan;
    }

    int createServer(Selector sel)
        throws IOException
    {
        ServerSocketChannel ssChan = ServerSocketChannel.open();
        ssChan.configureBlocking(false);
        ssChan.socket().setReuseAddress(true);

        ssChan.socket().bind(null);

        ssChan.register(sel, SelectionKey.OP_ACCEPT);

        return ssChan.socket().getLocalPort();
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite() {
        return new TestSuite(SimpleOutputEngineTest.class);
    }

    /**
     * Tears down the fixture, for example, close a network connection. This
     * method is called after a test is executed.
     *
     * @throws Exception if super class tearDown fails.
     */
    protected void tearDown()
        throws Exception
    {
        if (engine != null) {
            if (!engine.isDestroyed()) {
                if (engine.isError()) {
                    engine.clearError();
                }
                if (!engine.isStopped()) {
                    engine.forcedStopProcessing();
                }

                engine.destroyProcessor();
            }

            engine = null;
        }

        super.tearDown();
    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        engine = new SimpleOutputEngine("StartStop", 0, "test");
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        // try it a second time
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        // now try a stop message
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.sendLastAndStop();
        IOTestUtil.waitUntilStopped(engine, "send last");

        engine.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(engine, "send last");

        try {
            engine.startProcessing();
            fail("SimpleOutputEngine restart after kill succeeded");
        } catch (Exception e) {
            // expect this to fail
        }
    }

    public void testOutputLoop()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new VitreousBufferCache();

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        MockObserver observer = new MockObserver();

        engine = new SimpleOutputEngine("OutputLoop", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        final String notificationId = "OutputLoop";

        MockObserver xmitObserver = new MockObserver();
        xmitObserver.setSourceNotificationId(notificationId);

        OutputChannel transmitEng =
            engine.addDataChannel(testPipe.sink(), cacheMgr);
        transmitEng.registerComponentObserver(xmitObserver, notificationId);

/*
        assertEquals("Bad number of log messages", 1, getNumberOfMessages());
        assertEquals("Unexpected log message",
                     "Setting multiple observers", getMessage(0));
        clearMessages();
*/
assertEquals("Bad number of log messages", 0, getNumberOfMessages());

        assertTrue("SimpleOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        final int bufLen = 4096;

        // now move some buffers
        ByteBuffer testInBuf = ByteBuffer.allocate(bufLen * 2);

        ByteBuffer testOutBuf;
        for (int i = 0; i < 100; i++) {
            testOutBuf = cacheMgr.acquireBuffer(bufLen);
            assertNotNull("Unable to acquire buffer#" + i, testOutBuf);
            testOutBuf.putInt(0, bufLen);
            testOutBuf.limit(bufLen);
            testOutBuf.position(bufLen);
            testOutBuf.flip();

            transmitEng.receiveByteBuffer(testOutBuf);
            for (int j = 0; j < 100; j++) {
                if (!transmitEng.isOutputQueued()) {
                    break;
                }

                Thread.sleep(10);
            }
            assertFalse("PayloadTransmitChannel did not send buf#" + i,
                        transmitEng.isOutputQueued());

            testInBuf.position(0);
            testInBuf.limit(4);
            int numBytes = testPipe.source().read(testInBuf);
            assertEquals("Bad return count on read#" + i,
                         4, numBytes);
            assertEquals("Bad message byte count on read#" + i,
                         bufLen, testInBuf.getInt(0));

            testInBuf.limit(bufLen);
            int remBytes = testPipe.source().read(testInBuf);
            assertEquals("Bad remainder count on read#" + i,
                         bufLen - 4, remBytes);
        }

        engine.sendLastAndStop();
        transmitEng.flushOutQueue();
        IOTestUtil.waitUntilStopped(engine, "send last");

        assertTrue("Failure on sendLastAndStop command.",
                   observer.gotSourceStop());
        assertFalse("Got sinkStop notification",
                   observer.gotSinkStop());
        assertFalse("Got sourceError notification",
                   observer.gotSourceError());
        assertFalse("Got sinkError notification",
                   observer.gotSinkError());

        assertTrue("ByteBufferCache is not balanced", cacheMgr.isBalanced());
    }

    public void testServerOutput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new VitreousBufferCache();

        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver();

        engine = new SimpleOutputEngine("ServerOutput", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        final String notificationId = "ServerOutput";

        MockObserver xmitObserver = new MockObserver();
        xmitObserver.setSourceNotificationId(notificationId);

        OutputChannel transmitEng = engine.connect(cacheMgr, sock, 1);
        transmitEng.registerComponentObserver(xmitObserver, notificationId);

/*
        assertEquals("Bad number of log messages", 1, getNumberOfMessages());
        assertEquals("Unexpected log message",
                     "Setting multiple observers", getMessage(0));
        clearMessages();
*/
assertEquals("Bad number of log messages", 0, getNumberOfMessages());

        SocketChannel chan = acceptChannel(sel);

        assertTrue("SimpleOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        final int bufLen = 40;

        // now move some buffers
        ByteBuffer testInBuf = ByteBuffer.allocate(bufLen * 2);

        ByteBuffer testOutBuf;
        for (int i = 0; i < 100; i++) {
            testOutBuf = cacheMgr.acquireBuffer(bufLen);
            assertNotNull("Unable to acquire buffer#" + i, testOutBuf);
            testOutBuf.putInt(0, bufLen);
            testOutBuf.limit(bufLen);
            testOutBuf.position(bufLen);
            testOutBuf.flip();

            transmitEng.receiveByteBuffer(testOutBuf);
            for (int j = 0; j < 100; j++) {
                if (!transmitEng.isOutputQueued()) {
                    break;
                }

                Thread.sleep(10);
            }
            assertFalse("PayloadTransmitChannel did not send buf#" + i,
                        transmitEng.isOutputQueued());

            testInBuf.position(0);
            testInBuf.limit(4);
            int numBytes = chan.read(testInBuf);
            assertEquals("Bad return count on read#" + i,
                         4, numBytes);
            assertEquals("Bad message byte count on read#" + i,
                         bufLen, testInBuf.getInt(0));

            testInBuf.limit(bufLen);
            int remBytes = chan.read(testInBuf);
            assertEquals("Bad remainder count on read#" + i,
                         bufLen - 4, remBytes);
        }

        engine.sendLastAndStop();
        Thread.sleep(10);
        transmitEng.flushOutQueue();

        assertTrue("Failure on sendLastAndStop command.",
                   observer.gotSourceStop());
        assertFalse("Got sinkStop notification",
                   observer.gotSinkStop());
        assertFalse("Got sourceError notification",
                   observer.gotSourceError());
        assertFalse("Got sinkError notification",
                   observer.gotSinkError());

        assertTrue("ByteBufferCache is not balanced", cacheMgr.isBalanced());
    }

    public void testDisconnect()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new VitreousBufferCache();

        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver();

        engine = new SimpleOutputEngine("ServerOutput", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        final String notificationId = "ServerOutput";

        MockObserver xmitObserver = new MockObserver();
        xmitObserver.setSourceNotificationId(notificationId);

        OutputChannel transmitEng = engine.connect(cacheMgr, sock, 1);
        transmitEng.registerComponentObserver(xmitObserver, notificationId);

/*
        assertEquals("Bad number of log messages", 1, getNumberOfMessages());
        assertEquals("Unexpected log message",
                     "Setting multiple observers", getMessage(0));
        clearMessages();
*/
assertEquals("Bad number of log messages", 0, getNumberOfMessages());

        SocketChannel chan = acceptChannel(sel);

        assertTrue("SimpleOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.disconnect();
        Thread.sleep(10);

        assertFalse("Failure on sendLastAndStop command.",
                   observer.gotSourceStop());
        assertFalse("Got sinkStop notification",
                   observer.gotSinkStop());
        assertFalse("Got sourceError notification",
                   observer.gotSourceError());
        assertFalse("Got sinkError notification",
                   observer.gotSinkError());

        assertTrue("ByteBufferCache is not balanced", cacheMgr.isBalanced());
    }

    /**
     * Main routine which runs text test in standalone mode.
     *
     * @param args the arguments with which to execute this method.
     */
    public static void main(String[] args)
    {
        TestRunner.run(suite());
    }
}
