package icecube.daq.io;

import icecube.daq.io.test.IOTestUtil;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockObserver;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.VitreousBufferCache;
import icecube.daq.payload.impl.SourceID4B;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * SimpleDestinationOutputEngine unit tests.
 */
public class SimpleDestinationOutputEngineTest
    extends LoggingCase
{
    public SimpleDestinationOutputEngineTest(String name)
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
    public static Test suite()
    {
        return new TestSuite(SimpleDestinationOutputEngineTest.class);
    }

    public void testBasic()
    {
        SimpleDestinationOutputEngine eng =
            new SimpleDestinationOutputEngine("SDOE", 0, "testBasic");
        assertNull("Didn't expect a cache manager", eng.getBufferManager());
        assertEquals("Unexpected number of messages sent",
                     0, eng.getMessagesSent());

        IByteBufferCache cache = new VitreousBufferCache();
        eng.registerBufferManager(cache);
        assertEquals("Unexpected cache manager", cache, eng.getBufferManager());
    }

    public void testConnect()
        throws IOException
    {
        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver();

        SimpleDestinationOutputEngine engine =
            new SimpleDestinationOutputEngine("SDOE", 0, "testConnect");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        IByteBufferCache cacheMgr = new VitreousBufferCache();
        engine.registerBufferManager(cacheMgr);

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        SourceID4B srcId = new SourceID4B(1234);

        QueuedOutputChannel outChan =
            engine.connect(cacheMgr, sock, srcId.getSourceID());
        assertTrue("DestinationOutputEngine is not connected",
                   engine.isConnected());
        assertTrue("DestinationOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        assertNull("Didn't expect to find channel",
                   engine.lookUpEngineBySourceID(new SourceID4B(4321)));
        assertEquals("Expected to find channel",
                     outChan, engine.lookUpEngineBySourceID(srcId));

        SocketChannel chan = acceptChannel(sel);

        assertTrue("DestinationOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.payloadDestinationClosed(srcId);
        engine.allPayloadDestinationsClosed();

        engine.disconnect();
        IOTestUtil.waitUntilStopped(engine, "disconnect");

        assertTrue("DestinationOutputEngine is still connected",
                   !engine.isConnected());
        assertTrue("DestinationOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());
    }

    public void testSendData()
        throws IOException
    {
        Selector sel = Selector.open();

        int port = createServer(sel);

        MockObserver observer = new MockObserver();

        SimpleDestinationOutputEngine engine =
            new SimpleDestinationOutputEngine("SDOE", 0, "testSendData");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        IByteBufferCache cacheMgr = new VitreousBufferCache();
        engine.registerBufferManager(cacheMgr);

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        SourceID4B srcId = new SourceID4B(1234);

        QueuedOutputChannel outChan =
            engine.connect(cacheMgr, sock, srcId.getSourceID());
        assertTrue("DestinationOutputEngine is not connected",
                   engine.isConnected());
        assertTrue("DestinationOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        assertNull("Didn't expect to find channel",
                   engine.lookUpEngineBySourceID(new SourceID4B(4321)));
        assertEquals("Expected to find channel",
                     outChan, engine.lookUpEngineBySourceID(srcId));

        SocketChannel chan = acceptChannel(sel);

        assertTrue("DestinationOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());
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

            engine.sendPayload(srcId, testOutBuf);
            for (int j = 0; j < 100; j++) {
                if (!outChan.isOutputQueued()) {
                    break;
                }

                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    // ignore interrupts
                }
            }
            assertFalse("Channel did not send buf#" + i,
                        outChan.isOutputQueued());

            testInBuf.position(0);
            testInBuf.limit(4);
            int numBytes = chan.read(testInBuf);
            assertEquals("Bad return count on read#" + i, 4, numBytes);
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

        engine.disconnect();
        IOTestUtil.waitUntilStopped(engine, "disconnect");

        assertTrue("DestinationOutputEngine is still connected",
                   !engine.isConnected());
        assertTrue("DestinationOutputEngine in " + engine.getPresentState() +
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
