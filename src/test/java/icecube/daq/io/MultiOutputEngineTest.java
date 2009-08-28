package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.test.IOTestUtil;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.io.test.MockObserver;
import icecube.daq.io.test.MockWriteableChannel;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * MultiOutputEngine unit tests.
 */
public class MultiOutputEngineTest
    extends LoggingCase
{
    // private static member data
    private static final int BUFFER_BLEN = 32000;
    private static final String SRC_NOTIFICATION_ID = "SourceID";
    private static final String SRC_ERROR_ID = "SourceErrorID";

    /**
     * The object being tested.
     */
    private MultiOutputEngine engine;

    /**
     * Constructs and instance of this test.
     *
     * @param name the name of the test.
     */
    public MultiOutputEngineTest(String name)
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

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite() {
        return new TestSuite(MultiOutputEngineTest.class);
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
        if (engine != null && false) {
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
        final String engineName = "StartStop";
        final int engineId = 0;
        final String engineFcn = "test";

        int nextChan = 0;
        int chanNum;

        engine = new MultiOutputEngine(engineName, engineId, engineFcn);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        engine.addDataChannel(new MockWriteableChannel(), null);
        chanNum = nextChan++;

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // try it a second time
        engine.addDataChannel(new MockWriteableChannel(), null);
        chanNum = nextChan++;

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // now try a stop message
        engine.addDataChannel(new MockWriteableChannel(), null);
        chanNum = nextChan++;

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.sendLastAndStop();
        IOTestUtil.waitUntilStopped(engine, "send last");

        Thread.sleep(10);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // check out exception on channel close()
        MockWriteableChannel mockChan = new MockWriteableChannel();
        mockChan.setCloseException(new IOException("Mock close() exception"));

        engine.addDataChannel(mockChan, null);
        chanNum = nextChan++;

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.sendLastAndStop();
        IOTestUtil.waitUntilStopped(engine, "send last");

        Thread.sleep(10);

        assertEquals("Bad number of log messages",
                     1, getNumberOfMessages());
        assertEquals("Unexpected log message 0",
                     "Ignoring " + engineName + "#" + engineId + ":" +
                     engineFcn + ":" + chanNum + " close exception",
                     getMessage(0));
        clearMessages();

        engine.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            engine.startProcessing();
            fail("MultiOutputEngine restart after kill succeeded");
        } catch (Exception e) {
            // expect this to fail
        }

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());
    }

    public void testOutputLoop()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("OutLoop");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        testPipe.sink().configureBlocking(true);
        testPipe.source().configureBlocking(true);

        MockObserver observer = new MockObserver();

        engine = new MultiOutputEngine("OutputLoop", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        QueuedOutputChannel outChan =
            engine.addDataChannel(testPipe.sink(), cacheMgr);

        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
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

            outChan.receiveByteBuffer(testOutBuf);
            for (int j = 0; j < 100; j++) {
                if (!outChan.isOutputQueued()) {
                    break;
                }

                Thread.sleep(10);
            }
            assertFalse("PayloadTransmitChannel did not send buf#" + i,
                        outChan.isOutputQueued());

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
        outChan.flushOutQueue();

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

    public void testServerDisconnect()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("SrvrDis");

        Selector sel = Selector.open();

        int port = createServer(sel);

        //MockObserver observer = new MockObserver();

        engine = new MultiOutputEngine("ServerDisconnect", 0, "test");
        //engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(true);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        QueuedOutputChannel outChan = engine.connect(cacheMgr, sock, 1);
        assertTrue("MultiOutputEngine is not connected", engine.isConnected());
        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        SocketChannel chan = acceptChannel(sel);

        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.disconnect();
        IOTestUtil.waitUntilStopped(engine, "disconnect");

        assertTrue("MultiOutputEngine is still connected",
                   !engine.isConnected());
        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());
    }

    public void testServerOutput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("SrvrOut");

        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver();

        engine = new MultiOutputEngine("ServerOutput", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(true);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        QueuedOutputChannel outChan = engine.connect(cacheMgr, sock, 1);

        SocketChannel chan = acceptChannel(sel);

        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
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

            outChan.receiveByteBuffer(testOutBuf);
            for (int j = 0; j < 100; j++) {
                if (!outChan.isOutputQueued()) {
                    break;
                }

                Thread.sleep(10);
            }
            assertFalse("PayloadTransmitChannel did not send buf#" + i,
                        outChan.isOutputQueued());

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
        outChan.flushOutQueue();
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

    public void testWriteAndDisconnect()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new MockBufferCache("WrDis");

        Selector sel = Selector.open();

        int port = createServer(sel);

        //MockObserver observer = new MockObserver();

        engine = new MultiOutputEngine("WriteAndDisconnect", 0, "test");
        //engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(true);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        QueuedOutputChannel outChan = engine.connect(cacheMgr, sock, 1);
        assertTrue("MultiOutputEngine is not connected", engine.isConnected());
        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        SocketChannel chan = acceptChannel(sel);

        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        ByteBuffer testOutBuf;

        final int bufLen = 5;
        testOutBuf = cacheMgr.acquireBuffer(bufLen);
        assertNotNull("Unable to acquire buffer", testOutBuf);
        testOutBuf.putInt(0, bufLen);
        testOutBuf.limit(bufLen);
        testOutBuf.position(bufLen);
        testOutBuf.flip();

        outChan.receiveByteBuffer(testOutBuf);
        for (int j = 0; j < 100; j++) {
            if (!outChan.isOutputQueued()) {
                break;
            }

            Thread.sleep(10);
        }
        assertFalse("PayloadTransmitChannel did not send buf",
                    outChan.isOutputQueued());

        engine.disconnect();
        IOTestUtil.waitUntilStopped(engine, "disconnect");

        assertTrue("MultiOutputEngine is still connected",
                   !engine.isConnected());
        assertTrue("MultiOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());
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
