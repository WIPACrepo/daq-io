package icecube.daq.io;

import icecube.daq.io.test.IOTestUtil;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.io.test.MockObserver;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

class SimpleTestReader
    extends SimpleReader
{
    private ArrayList<ByteBuffer> inputData = new ArrayList<ByteBuffer>();

    class TestChannel
        extends SimpleChannel
    {
        TestChannel(IOChannelParent parent, String name,
                    SelectableChannel channel, IByteBufferCache bufMgr,
                    int bufSize)
            throws IOException
        {
            super(parent, name, channel, bufMgr, bufSize);
        }

        public void pushPayload(ByteBuffer buf)
        {
            synchronized (inputData) {
                inputData.add(buf);
            }
        }
    }

    SimpleTestReader(String name)
        throws IOException
    {
        super(name);
    }

    public SimpleChannel createChannel(String name, SelectableChannel channel,
                                       IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        return new TestChannel(this, name, channel, bufMgr, bufSize);
    }

    boolean hasPayloads()
    {
        return inputData.size() > 0;
    }

    List<ByteBuffer> getPayloads()
    {
        ArrayList list = new ArrayList();

        synchronized (inputData) {
            list.addAll(inputData);
            inputData.clear();
        }

        return list;
    }
}

public class SimpleReaderTest
    extends LoggingCase
{
    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

    private SimpleTestReader tstRdr;

    /**
     * Constructs an instance of this test.
     *
     * @param name the name of the test.
     */
    public SimpleReaderTest(String name)
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

    private static final void checkGetters(SimpleReader rdr,
                                           IByteBufferCache bufMgr,
                                           int numReceiveChans,
                                           long bufsAcquired,
                                           long bytesAcquired,
                                           long bytesRcvd, long recsRcvd,
                                           long stopsRcvd)
    {
        boolean[] allocStopped = rdr.getAllocationStopped();
        assertNotNull("Got null allocationStopped array", allocStopped);
        assertEquals("Bad allocationStopped length",
                     numReceiveChans, allocStopped.length);
        if (numReceiveChans > 0) {
            assertFalse("allocationStopped[0] was not false",
                        allocStopped[0]);
        }

        long[] bufArray = new long[1];

        for (int i = 0; i < 3; i++) {
            long[] data;
            String name;
            long val;

            switch (i) {
            case 0:
                name = "bytesRcvd";
                data = rdr.getBytesReceived();
                val = bytesRcvd;
                break;
            case 1:
                name = "recsRcvd";
                data = rdr.getRecordsReceived();
                val = recsRcvd;
                break;
            case 2:
                name = "stopsRcvd";
                data = rdr.getStopMessagesReceived();
                val = stopsRcvd;
                break;
            case 3:
                name = "curAcqBuf";
                bufArray[0] = bufMgr.getCurrentAquiredBuffers();
                data = bufArray;
                val = bufsAcquired;
                break;
            case 4:
                name = "curAcqByt";
                bufArray[0] = bufMgr.getCurrentAquiredBytes();
                data = bufArray;
                val = bytesAcquired;
                break;
/*
            case 5:
                name = "lim2Rest";
                data = rdr.getLimitToRestartAllocation();
                val = 100000000;
                break;
            case 6:
                name = "lim2Stop";
                data = rdr.getLimitToStopAllocation();
                val = 140000000;
                break;
*/
            default:
                name = "unknown";
                data = null;
                val = Long.MAX_VALUE;
                break;
            }

            assertNotNull("Got null " + name + " array", data);
            assertEquals("Bad " + name + " length",
                         numReceiveChans, data.length);
            if (numReceiveChans > 0) {
                assertEquals("Bad " + name + "[0] value", val, data[0]);
            }
        }
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

    private static final int harvestBuffers(SimpleTestReader tstRdr,
                                            int bufLen,
                                            IByteBufferCache bufMgr)
    {
        return harvestBuffers(tstRdr, bufLen, bufMgr, false, 0);
    }

    private static final int harvestBuffers(SimpleTestReader tstRdr,
                                            int bufLen,
                                            IByteBufferCache bufMgr,
                                            boolean checkId, int prevId)
    {
        if (!tstRdr.hasPayloads()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }

            return 0;
        }

        int numHarvested = 0;
        for (ByteBuffer buf : tstRdr.getPayloads()) {
            assertEquals("Bad payload length", bufLen, buf.getInt(0));
            assertEquals("Bad buffer position",
                         0, buf.position());
            assertEquals("Bad buffer position",
                         bufLen, buf.limit());

            if (checkId) {
                int newId = buf.getInt(4);
                assertEquals("Bad buffer ID", prevId + 1, newId);
                prevId = newId;
            }

            bufMgr.returnBuffer(buf);

            numHarvested++;
        }

        return numHarvested;
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        tstRdr = null;
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(SimpleReaderTest.class);
    }

    protected void tearDown()
        throws Exception
    {
        if (tstRdr != null) {
            tstRdr.destroyProcessor();
        }

        super.tearDown();
    }

    public void testBasic()
        throws IOException
    {
        IByteBufferCache bufMgr = new MockBufferCache("Basic");

        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();

        tstRdr = new SimpleTestReader("Basic");

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr, 256);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < INPUT_OUTPUT_LOOP_CNT) {
            if (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                final int acquireLen = bufLen;
                testBuf = bufMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);

                bufMgr.returnBuffer(testBuf);

                xmitCnt++;
            }

            recvCnt += harvestBuffers(tstRdr, bufLen, bufMgr);

            loopCnt++;
            if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }
    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        tstRdr = new SimpleTestReader("StartStop");

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // try it a second time
        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        tstRdr.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(tstRdr);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            tstRdr.startProcessing();
            fail("Reader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }
    }

    public void testOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("OutIn");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockObserver observer = new MockObserver();

        tstRdr = new SimpleTestReader("OutputInput");
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < INPUT_OUTPUT_LOOP_CNT) {
            if (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                final int acquireLen = bufLen;
                testBuf = bufMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);

                bufMgr.returnBuffer(testBuf);

                xmitCnt++;
            }

            recvCnt += harvestBuffers(tstRdr, bufLen, bufMgr);

            loopCnt++;
            if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop", observer.gotSinkStop());
    }

    public void testMultiOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("MultiOutIn");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockObserver observer = new MockObserver();

        tstRdr = new SimpleTestReader("MultiOutputInput");
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;
        final int groupSize = 3;

        final int numToSend = INPUT_OUTPUT_LOOP_CNT * groupSize;

        int id = 1;
        int recvId = 0;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < numToSend) {
            if (xmitCnt < numToSend) {
                final int acquireLen = bufLen * groupSize;
                testBuf = bufMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try", testBuf);

                for (int i = 0; i < groupSize; i++) {
                    final int start = bufLen * i;
                    testBuf.putInt(start, bufLen);
                    testBuf.putInt(start + 4, id++);
                }
                testBuf.limit(acquireLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);

                bufMgr.returnBuffer(testBuf);

                xmitCnt += groupSize;
            } else {
                try {
                    Thread.sleep(100);
                } catch (Exception ex) {
                    // ignore interrupts
                }
            }

            int numBufs = harvestBuffers(tstRdr, bufLen, bufMgr, true, recvId);
            if (numBufs > 0) {
                recvId += numBufs;
                recvCnt += numBufs;
            }

            loopCnt++;
            if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop", observer.gotSinkStop());
    }

    public void testMultiSizeOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("MultiSzOutIn");

        MockObserver observer = new MockObserver();

        tstRdr = new SimpleTestReader("MultiSize");
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        // avoid a Sun race condition
        try {
            Thread.sleep(100);
        } catch (Exception ex) {
            // ignore interrupts
        }

        for (int msgSize = 10; msgSize <= 13; msgSize++) {
            for (int bufLen = 32; bufLen <= 40; bufLen++) {
                // create a pipe for use in testing
                Pipe testPipe = Pipe.open();
                Pipe.SinkChannel sinkChannel = testPipe.sink();
                sinkChannel.configureBlocking(false);

                Pipe.SourceChannel sourceChannel = testPipe.source();
                sourceChannel.configureBlocking(false);

                tstRdr.addDataChannel(sourceChannel, bufMgr, bufLen);

                tstRdr.startProcessing();
                IOTestUtil.waitUntilRunning(tstRdr, " (msgSize " + msgSize +
                                            ", bufLen " + bufLen + ")");

                assertEquals("There are acquired byte buffers before start" +
                             " (msgSize " + msgSize + ", bufLen " + bufLen +
                             ")", 0, bufMgr.getCurrentAquiredBuffers());

                // now move some buffers
                ByteBuffer testBuf;

                final int groupSize = 3;

                int id = 1;
                int recvId = 0;

                int xmitCnt = 0;
                int recvCnt = 0;
                int loopCnt = 0;
                while (recvCnt < INPUT_OUTPUT_LOOP_CNT * groupSize) {
                    if (xmitCnt < INPUT_OUTPUT_LOOP_CNT * groupSize) {
                        final int acquireLen = msgSize * groupSize;
                        testBuf = bufMgr.acquireBuffer(acquireLen);
                        assertNotNull("Unable to acquire transmit buffer on " +
                                      xmitCnt + " try", testBuf);

                        for (int i = 0; i < groupSize; i++) {
                            final int start = msgSize * i;
                            testBuf.putInt(start, msgSize);
                            testBuf.putInt(start + 4, id++);
                        }
                        testBuf.limit(acquireLen);
                        testBuf.position(0);
                        sinkChannel.write(testBuf);

                        bufMgr.returnBuffer(testBuf);

                        xmitCnt += groupSize;
                    } else {
                        try {
                            Thread.sleep(10);
                        } catch (Exception ex) {
                            // ignore interrupts
                        }
                    }

                    int numBufs = harvestBuffers(tstRdr, msgSize, bufMgr,
                                                 true, recvId);
                    if (numBufs > 0) {
                        recvId += numBufs;
                        recvCnt += numBufs;
                    }

                    loopCnt++;
                    if (loopCnt == (recvCnt + INPUT_OUTPUT_LOOP_CNT) * 10) {
                        fail("Received " + recvCnt + " payloads after " +
                             xmitCnt + " buffers were transmitted (msgSize " +
                             msgSize + ", bufLen " + bufLen + ")");
                    }
                }

                IOTestUtil.sendStopMsg(sinkChannel);
                IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
                assertTrue("Observer didn't see sinkStop",
                           observer.gotSinkStop());

                for (int i = 0; i < 5; i++) {
                    if (bufMgr.getCurrentAquiredBuffers() == 0) {
                        break;
                    }

                    try {
System.err.println("sleep#"+i+" BM "+bufMgr);
                        Thread.sleep(100);
                    } catch (Exception ex) {
                        // ignore exceptions
                    }
                }

                assertEquals("There are still unreturned byte buffers",
                             0, bufMgr.getCurrentAquiredBuffers());
            }
        }
    }

    public void testDisposing()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("Disp");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockObserver observer = new MockObserver();

        tstRdr = new SimpleTestReader("Disposing");
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        for (int i = 0; i < 5; i++) {
            final int acquireLen = bufLen;
            testBuf = bufMgr.acquireBuffer(acquireLen);
            assertNotNull("Unable to acquire transmit buffer on try #" + i,
                          testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(0);
            sinkChannel.write(testBuf);

            bufMgr.returnBuffer(testBuf);

            if (i == 1) {
                tstRdr.startDisposing();
                IOTestUtil.waitUntilDisposing(tstRdr);
            }

            harvestBuffers(tstRdr, bufLen, bufMgr);
        }

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop", observer.gotSinkStop());
    }

    public void testGetters()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("Get");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockObserver observer = new MockObserver();

        tstRdr = new SimpleTestReader("Getters");
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        checkGetters(tstRdr, bufMgr, 1, 0, 0, 0, 0, 0);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        final int acquireLen = bufLen;
        testBuf = bufMgr.acquireBuffer(acquireLen);
        assertNotNull("Unable to acquire transmit buffer", testBuf);

        checkGetters(tstRdr, bufMgr, 1, 1, acquireLen, 0, 0, 0);

        testBuf.putInt(0, bufLen);
        testBuf.limit(bufLen);
        testBuf.position(bufLen);
        testBuf.flip();

        sinkChannel.write(testBuf);

        bufMgr.returnBuffer(testBuf);

        for (int i = 0; tstRdr.getRecordsReceived()[0] == 0 && i < 10; i++) {
            Thread.sleep(100);
        }

        checkGetters(tstRdr, bufMgr, 1, 1, acquireLen, bufLen, 1, 0);

        assertFalse("Reader in Error state after ErrorSig",
                    tstRdr.isError());
        assertFalse("Observer saw sinkError", observer.gotSinkError());

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop", observer.gotSinkStop());

        checkGetters(tstRdr, bufMgr, 0, 0, BUFFER_LEN, bufLen, 1, 1);
    }

    /**
     * Test starting and stopping server version of input tstRdr.
     */
    public void testInetServer()
        throws Exception
    {
        IByteBufferCache bufMgr = new MockBufferCache("InetSrvr");

        tstRdr = new SimpleTestReader("InetServer");

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.startServer(bufMgr);
        waitUntilServerStarted(tstRdr);

        InetSocketAddress addr =
            new InetSocketAddress("localhost", tstRdr.getServerPort());

        SocketChannel chan = SocketChannel.open(addr);

        Thread.sleep(100);

        // this socket will die in the middle of the show
        SocketChannel redShirt = SocketChannel.open(addr);

        Thread.sleep(100);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
            final int acquireLen = bufLen;
            testBuf = bufMgr.acquireBuffer(acquireLen);
            assertNotNull("Unable to acquire transmit buffer on " +
                          xmitCnt + " try", testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chan.write(testBuf);
            if (xmitCnt < 2) {
                testBuf.position(0);
                redShirt.write(testBuf);
            } else {
                redShirt.close();
            }

            bufMgr.returnBuffer(testBuf);

            xmitCnt++;
            Thread.sleep(100);

            // XXX should check recvCnt
        }


        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // try it a second time
        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        tstRdr.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(tstRdr);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            tstRdr.startProcessing();
            fail("Reader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }
    }

    /**
     * Test multiple input engine servers.
     */
    public void testMultiServer()
        throws Exception
    {
        IByteBufferCache bufMgr = new MockBufferCache("MultiSrvr");

        final int numTstRdrs = 4;

        SimpleReader[] tstRdrs = new SimpleTestReader[numTstRdrs];

        // create a bunch of engines
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i] = new SimpleTestReader("MultiServer");

            tstRdrs[i].start();
            IOTestUtil.waitUntilStopped(tstRdrs[i], "creation",
                                        " (#" + i + ")");
        }

        // start all the servers
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].startServer(bufMgr);
            waitUntilServerStarted(tstRdrs[i], " (#" + i + ")");
        }

        SocketChannel[] chans = new SocketChannel[numTstRdrs];

        // open a channel to each engine
        for (int i = 0; i < numTstRdrs; i++) {
            InetSocketAddress addr =
                new InetSocketAddress("localhost", tstRdrs[i].getServerPort());
            chans[i] = SocketChannel.open(addr);
        }

        Thread.sleep(100);

        // gentlemen, start your engines
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].startProcessing();
        }
        for (int i = 0; i < numTstRdrs; i++) {
            IOTestUtil.waitUntilRunning(tstRdrs[i], " (#" + i + ")");
        }

        ByteBuffer testBuf;

        final int bufLen = 64;

        for (int i = 0; i < numTstRdrs; i++) {
            testBuf = bufMgr.acquireBuffer(bufLen);
            assertNotNull("Unable to acquire transmit buffer#" + i, testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chans[i].write(testBuf);

            bufMgr.returnBuffer(testBuf);
        }

        Thread.sleep(100);

        boolean gotAll = false;

        // wait until we've got data on all channels
        final int numTries = 5;
        for (int i = 0; !gotAll && i < numTries; i++) {
            boolean rcvdData = true;
            for (int j = 0; rcvdData && j < numTstRdrs; j++) {
                long[] rcvd = tstRdrs[j].getBytesReceived();
                assertNotNull("Got null byteRcvd array from engine#" + j,
                              rcvd);
                assertEquals("Unexpected number of connections for tstRdr#" + j,
                             1, rcvd.length);
                if (rcvd[0] < bufLen) {
                    rcvdData = false;
                }
            }

            // if we've got data in all engines, we're done
            if (rcvdData) {
                gotAll = true;
            } else {
                Thread.sleep(100);
            }
        }

        // stop everything
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].forcedStopProcessing();
        }
        for (int i = 0; i < numTstRdrs; i++) {
            IOTestUtil.waitUntilStopped(tstRdrs[i], "forced stop",
                                        " (#" + i + ")");
        }

        // destroy everything
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].destroyProcessor();
        }
        for (int i = 0; i < numTstRdrs; i++) {
            IOTestUtil.waitUntilDestroyed(tstRdrs[i], " (#" + i + ")");
        }
    }

    public void testServerInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("SrvrIn");

        Selector sel = Selector.open();

        int port = createServer(sel);

        tstRdr = new SimpleTestReader("ServerInput");
        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addReverseConnection("localhost", port, bufMgr);
        IOTestUtil.waitUntilStopped(tstRdr, "reverse connection");

        ByteBuffer testBuf;

        for (int i = 0; i < 2; i++) {
            tstRdr.startProcessing();
            IOTestUtil.waitUntilRunning(tstRdr);

            SocketChannel chan = acceptChannel(sel);

            final int bufLen = 40;

            testBuf = bufMgr.acquireBuffer(bufLen);
            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chan.write(testBuf);

            bufMgr.returnBuffer(testBuf);

            // wait until we've got data on all channels
            boolean gotAll = false;
            final int numTries = 5;
            for (int t = 0; !gotAll && t < numTries; t++) {
                boolean rcvdData = true;
                long[] rcvd = tstRdr.getBytesReceived();
                assertNotNull("Got null byteRcvd array from engine", rcvd);
                assertEquals("Unexpected number of connections for engine",
                             1, rcvd.length);
                if (rcvd[0] < bufLen) {
                    rcvdData = false;
                }

                // if we've got data, we're done
                if (rcvdData) {
                    gotAll = true;
                } else {
                    Thread.sleep(100);
                }
            }

            long[] totRcvd = tstRdr.getBytesReceived();
            assertNotNull("Got null byteRcvd array from engine", totRcvd);
            assertEquals("Unexpected number of connections for engine",
                         1, totRcvd.length);
            assertEquals("Bad number of bytes", bufLen, totRcvd[0]);

            testBuf = bufMgr.acquireBuffer(4);
            assertNotNull("Unable to acquire stop buffer", testBuf);

            testBuf.putInt(4);
            testBuf.limit(4);
            testBuf.position(4);
            testBuf.flip();

            chan.write(testBuf);

            bufMgr.returnBuffer(testBuf);

            IOTestUtil.waitUntilStopped(tstRdr, "stop msg");

            // make sure receive engines have been detatched
            long[] postRcvd = tstRdr.getBytesReceived();
            assertNotNull("Got null byteRcvd array from engine", postRcvd);
            assertEquals("Unexpected number of connections for engine",
                         0, postRcvd.length);
        }

        tstRdr.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(tstRdr);
    }

    public void testAllocRestart()
        throws Exception
    {
        final int bufLen = 64;
        final int maxLoopCnt = 15;

        // buffer caching manager
        IByteBufferCache bufMgr =
            new MockBufferCache("AllocRe", (long) (bufLen * 4));

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockObserver observer = new MockObserver();

        tstRdr = new SimpleTestReader("OutputInput");
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        // now move some buffers
        ByteBuffer testBuf;

        int id = 0;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < maxLoopCnt) {
            if (xmitCnt < maxLoopCnt) {
                final int acquireLen = bufLen;
                testBuf = bufMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.putInt(4, ++id);
                testBuf.limit(bufLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);

                bufMgr.returnBuffer(testBuf);

                xmitCnt++;
            }

            boolean allocStopped = tstRdr.getAllocationStopped()[0];
            if (allocStopped || (xmitCnt - recvCnt) > (maxLoopCnt / 2) ||
                (xmitCnt == maxLoopCnt))
            {
                recvCnt += harvestBuffers(tstRdr, bufLen, bufMgr,
                                          true, recvCnt);

                if (allocStopped || xmitCnt == maxLoopCnt) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        // ignore interrupts
                    }
                }
            }

            loopCnt++;
            if (loopCnt == recvCnt + maxLoopCnt) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop", observer.gotSinkStop());

        // don't even bother checking stop/restart log msgs
        clearMessages();
    }

    private static final void waitUntilServerStarted(SimpleReader rdr)
    {
        waitUntilServerStarted(rdr, "");
    }

    private static final void waitUntilServerStarted(SimpleReader rdr,
                                                     String extra)
    {
        for (int i = 0; i < 5 && !rdr.isServerStarted(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("Server thread has not started" + extra,
                   rdr.isServerStarted());
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
