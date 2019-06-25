/*
 * class: SimpleOutputEngineTest
 *
 * Version $Id: SimpleOutputEngineTest.java 2848 2008-03-25 22:03:17Z dglo $
 *
 * Date: May 19 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.test.IOTestUtil;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.io.test.MockObserver;
import icecube.daq.payload.IByteBufferCache;

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

    @Override
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
    @Override
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
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("StartStop");

        QueuedOutputChannel transmitEng;
        Pipe testPipe;

        // create a pipe for use in testing
        testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        engine = new SimpleOutputEngine("StartStop", 0, "test");
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        transmitEng = engine.addDataChannel(testPipe.sink(), cacheMgr,
                                            "SSChan");

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        testPipe.sink().close();
        testPipe.source().close();

        // try it a second time

        testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        transmitEng = engine.addDataChannel(testPipe.sink(), cacheMgr,
                                            "SSChan2");

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        // now try a stop message

        testPipe.sink().close();
        testPipe.source().close();

        testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        transmitEng = engine.addDataChannel(testPipe.sink(), cacheMgr,
                                            "SSStop");

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
        IByteBufferCache cacheMgr = new MockBufferCache("OutLoop");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        MockObserver observer = new MockObserver("OutputLoop");

        engine = new SimpleOutputEngine("OutputLoop", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        assertNoLogMessages();

        final String notificationId = "OutputLoop";

        QueuedOutputChannel transmitEng =
            engine.addDataChannel(testPipe.sink(), cacheMgr, "SSOut");

        assertNoLogMessages();

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

    public void testBrokenPipe()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("SrvrOut");

        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver("BrokenPipe");

        engine = new SimpleOutputEngine("ServerOutput", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertNoLogMessages();

        final String notificationId = "ServerOutput";

        QueuedOutputChannel transmitEng = engine.connect(cacheMgr, sock, 1);

        assertNoLogMessages();

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

            if (i < 50) {
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
            } else if (chan.isOpen()) {
                chan.close();
            }
        }

        engine.sendLastAndStop();
        IOTestUtil.waitUntilStopped(engine, "send last");
        transmitEng.flushOutQueue();

        IOTestUtil.waitUntilStopped(engine, "finished");

        assertTrue("Failure on sendLastAndStop command.",
                   observer.gotSourceStop());
        assertFalse("Got sinkStop notification",
                   observer.gotSinkStop());
        assertFalse("Got sourceError notification",
                   observer.gotSourceError());
        assertFalse("Got sinkError notification",
                   observer.gotSinkError());

        assertTrue("ByteBufferCache is not balanced", cacheMgr.isBalanced());

        for (int i = 0; i < getNumberOfMessages(); i++) {
            String msg = (String) getMessage(i);
            assertTrue("Bad log message \"" + msg + "\"",
                       msg.startsWith("Channel ") && msg.endsWith(" failed"));
        }
        clearMessages();
    }

    public void testServerOutput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("SrvrOut");

        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver("ServerOutput");

        engine = new SimpleOutputEngine("ServerOutput", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertNoLogMessages();

        final String notificationId = "ServerOutput";

        QueuedOutputChannel transmitEng = engine.connect(cacheMgr, sock, 1);

        assertNoLogMessages();

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
        IOTestUtil.waitUntilStopped(engine, "send last");
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
        String testName = "Disconnect";

        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("Disconn");

        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver("Disconnect");

        engine = new SimpleOutputEngine(testName, 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertNoLogMessages();

        QueuedOutputChannel transmitEng = engine.connect(cacheMgr, sock, 1);

        assertNoLogMessages();

        SocketChannel chan = acceptChannel(sel);

        assertTrue("SimpleOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.disconnect();
        IOTestUtil.waitUntilStopped(engine, "disconnect");

        assertTrue("Failure on sendLastAndStop command: " + observer,
                   observer.gotSourceStop());
        assertFalse("Got sinkStop notification: " + observer,
                   observer.gotSinkStop());
        assertFalse("Got sourceError notification: " + observer,
                   observer.gotSourceError());
        assertFalse("Got sinkError notification: " + observer,
                   observer.gotSinkError());

        assertTrue("ByteBufferCache is not balanced", cacheMgr.isBalanced());
    }

    class BufferWriter
        implements Runnable
    {
        private QueuedOutputChannel chan;
        private IByteBufferCache cache;
        private int bufLen;
        private int numToSend;

        private int received;

        private Thread thread;

        BufferWriter(QueuedOutputChannel chan, IByteBufferCache cache,
                     int bufLen, int numToSend)
        {
            this.chan = chan;
            this.cache = cache;
            this.bufLen = bufLen;
            this.numToSend = numToSend;
        }

        public int getNumReceived()
        {
            return received;
        }

        @Override
        public void run()
        {
            ByteBuffer buf;
            for (int i = 0; i < numToSend; i++) {
                buf = cache.acquireBuffer(bufLen);
                assertNotNull("Unable to acquire buffer#" + i, buf);
                buf.putInt(0, bufLen);
                buf.limit(bufLen);
                buf.position(bufLen);
                buf.flip();

                chan.receiveByteBuffer(buf);
                received++;
            }
        }

        public void start()
        {
            thread = new Thread(this);
            thread.setName(getClass().getName() + ": " + chan);
            thread.start();
        }
    }

    public void testOutputPause()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("OutPause");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        MockObserver observer = new MockObserver("OutputPause");

        engine = new SimpleOutputEngine("OutputPause", 0, "test", 10);
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        assertNoLogMessages();

        final String notificationId = "OutputPause";

        QueuedOutputChannel transmitEng =
            engine.addDataChannel(testPipe.sink(), cacheMgr, "SSOut");

        assertNoLogMessages();

        assertTrue("SimpleOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        final int bufLen = 4096;
        final int numSent = 100;

        // start a thread to fill the output queue
        BufferWriter out = new BufferWriter(transmitEng, cacheMgr, bufLen,
                                            numSent);
        out.start();

        // wait for output engine to pause input
        int prevRcvd = 0;
        while (!transmitEng.isOutputPaused()) {
            final int rcvd = out.getNumReceived();
            if (rcvd >= numSent) {
                break;
            }

            Thread.sleep(10);
        }

        // read everything from the output engine
        ByteBuffer testInBuf = ByteBuffer.allocate(bufLen * 2);
        for (int i = 0; i < numSent; i++) {
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

        for (int i = 0; i < getNumberOfMessages(); i++) {
            String msg = (String) getMessage(i);
            assertTrue("Bad log message \"" + msg + "\"",
                       msg.startsWith("Pausing ") ||
                       msg.startsWith("Resuming "));
        }
        clearMessages();

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
