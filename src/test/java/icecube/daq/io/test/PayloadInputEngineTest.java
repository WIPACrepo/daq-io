/*
 * class: PayloadInputEngineTest
 *
 * Version $Id: PayloadInputEngineTest.java,v 1.4 2006/06/30 18:07:04 dwharton Exp $
 *
 * Date: May 19 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io.test;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import icecube.daq.common.*;
import icecube.daq.io.PayloadInputEngine;
import icecube.daq.io.PayloadOutputEngine;
import icecube.daq.io.PayloadTransmitChannel;
import icecube.daq.io.PayloadReceiveChannel;
import icecube.daq.payload.ByteBufferCache;
import icecube.daq.payload.IByteBufferCache;
//import icecube.daq.payload.TriggerUtilConstants;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * This class defines the tests that any PayloadInputEngine object should pass.
 *
 * @author mcp
 * @version $Id: PayloadInputEngineTest.java,v 1.4 2006/06/30 18:07:04 dwharton Exp $
 */
public class PayloadInputEngineTest
    extends TestCase
    implements DAQComponentObserver
{
    private Log LOG = LogFactory.getLog(PayloadInputEngineTest.class);

    private static final int INPUT_OUTPUT_LOOP_CNT = 5;
    private static final int NUM_BUFFERS = 100;
    private static final int BUFFER_BLEN = 5000;
    private static final String SINK_NOTIFICATION_ID = "SinkID";
    private static final String SINK_ERROR_NOTIFICATION_ID = "SinkErrorID";
    private static final String SOURCE_NOTIFICATION_ID = "SourceID";
    private static final String SOURCE_ERROR_NOTIFICATION_ID = "SourceErrorID";

    private static Level logLevel = Level.INFO;

    private boolean sinkStopNotificationCalled;
    private boolean sinkErrorNotificationCalled;
    private boolean sourceStopNotificationCalled;
    private boolean sourceErrorNotificationCalled;

    private Object lock = new Object();

    /**
     * The object being tested.
     */
    private PayloadInputEngine engine;
    private PayloadOutputEngine testOutput;

    /**
     * Constructs an instance of this test.
     *
     * @param name the name of the test.
     */
    public PayloadInputEngineTest(String name)
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

    private static final void checkGetters(PayloadInputEngine engine,
                                           int numReceiveChans,
                                           long bufsAcquired,
                                           long bytesAcquired,
                                           long bytesRcvd, long recsRcvd,
                                           long stopsRcvd)
    {
        Boolean[] allocStopped = engine.getAllocationStopped();
        assertNotNull("Got null allocationStopped array", allocStopped);
        assertEquals("Bad allocationStopped length",
                     numReceiveChans, allocStopped.length);
        if (numReceiveChans > 0) {
            assertFalse("allocationStopped[0] was not false",
                        allocStopped[0].booleanValue());
        }

        for (int i = 0; i < 9; i++) {
            Long[] data;
            String name;
            long val;

            switch (i) {
            case 0:
                name = "curAcqBuf";
                data = engine.getBufferCurrentAcquiredBuffers();
                val = bufsAcquired;
            break;
            case 1:
                name = "curAcqByt";
                data = engine.getBufferCurrentAcquiredBytes();
                val = bytesAcquired;
                break;
            case 2:
                name = "bytesRcvd";
                data = engine.getBytesReceived();
                val = bytesRcvd;
                break;
            case 3:
                name = "lim2Rest";
                data = engine.getLimitToRestartAllocation();
                val = 100000;
                break;
            case 4:
                name = "lim2Stop";
                data = engine.getLimitToStopAllocation();
                val = 140000;
                break;
            case 5:
                name = "maxRest";
                data = engine.getPercentMaxRestartAllocation();
                val = 50;
                break;
            case 6:
                name = "maxStop";
                data = engine.getPercentMaxStopAllocation();
                val = 70;
                break;
            case 7:
                name = "recsRcvd";
                data = engine.getRecordsReceived();
                val = recsRcvd;
                break;
            case 8:
                name = "stopsRcvd";
                data = engine.getStopMessagesReceived();
                val = stopsRcvd;
                break;
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
                assertEquals("Bad " + name + "[0] value",
                             val, data[0].longValue());
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

    private static final void dumpGetters(PayloadInputEngine engine)
    {
        System.err.println("========================================");

        Boolean[] allocStopped = engine.getAllocationStopped();
        System.err.print("allocStopped:");
        for (int j = 0; j < allocStopped.length; j++) {
            System.err.print(" " + allocStopped[j]);
        }
        System.err.println();

        for (int i = 0; i < 9; i++) {
            Long[] data;
            String name;
            switch (i) {
            case 0:
                name = "curAcqBuf";
                data = engine.getBufferCurrentAcquiredBuffers();
                break;
            case 1:
                name = "curAcqByt";
                data = engine.getBufferCurrentAcquiredBytes();
                break;
            case 2:
                name = "bytesRcvd";
                data = engine.getBytesReceived();
                break;
            case 3:
                name = "lim2Rest";
                data = engine.getLimitToRestartAllocation();
                break;
            case 4:
                name = "lim2Stop";
                data = engine.getLimitToStopAllocation();
                break;
            case 5:
                name = "maxRest";
                data = engine.getPercentMaxRestartAllocation();
                break;
            case 6:
                name = "maxStop";
                data = engine.getPercentMaxStopAllocation();
                break;
            case 7:
                name = "recsRcvd";
                data = engine.getRecordsReceived();
                break;
            case 8:
                name = "stopsRcvd";
                data = engine.getStopMessagesReceived();
                break;
            default:
                name = "unknown";
                data = null;
                break;
            }

            System.err.print(name + ":");
            for (int j = 0; j < data.length; j++) {
                System.err.print(" " + data[j]);
            }
            System.err.println();
        }

        System.err.println("presState: " + engine.getPresentState());
        String[] presChanStates = engine.getPresentChannelStates();
        System.err.print("chanStates:");
        for (int j = 0; j < presChanStates.length; j++) {
            System.err.print(" " + presChanStates[j]);
        }
        System.err.println();

        System.err.println("========================================");
    }

    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     *
     * @throws Exception if super class setUp fails.
     */
    protected void setUp()
        throws Exception
    {
        super.setUp();

        sinkStopNotificationCalled = false;
        sinkErrorNotificationCalled = false;
        sourceStopNotificationCalled = false;
        sourceErrorNotificationCalled = false;

        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(new MockAppender(logLevel));
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(PayloadInputEngineTest.class);
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
                if (!engine.isStopped()) {
                    engine.forcedStopProcessing();
                }

                engine.destroyProcessor();
            }

            engine = null;
        }

        if (testOutput != null) {
            if (!testOutput.isDestroyed()) {
                if (!testOutput.isStopped()) {
                    testOutput.forcedStopProcessing();
                }

                testOutput.destroyProcessor();
            }

            testOutput = null;
        }

        super.tearDown();
    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        engine = new PayloadInputEngine("StartStop", 0, "test");
        engine.start();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();

        for (int i = 0; i < 5 && !engine.isRunning(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        engine.forcedStopProcessing();

        for (int i = 0; i < 5 && !engine.isStopped(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());

        // try it a second time
        engine.startProcessing();

        for (int i = 0; i < 5 && !engine.isRunning(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        engine.forcedStopProcessing();

        for (int i = 0; i < 5 && !engine.isStopped(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());

        engine.destroyProcessor();

        for (int i = 0; i < 5 && !engine.isDestroyed(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine did not die after kill request",
                   engine.isDestroyed());

        try {
            engine.startProcessing();
            fail("PayloadInputEngine restart after kill succeeded");
        } catch (Exception e) {
            // expect this to fail
        }
    }

    /**
     * Test starting and disposing input engine.
     */
    public void testStartDispose()
        throws Exception
    {
        engine = new PayloadInputEngine("StartDispose", 0, "test");
        engine.start();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();

        for (int i = 0; i < 5 && !engine.isRunning(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        engine.startDisposing();

        for (int i = 0; i < 5 && !engine.isRunning(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after StartDisposing",
                   engine.isRunning());
    }

    public void testOutputInput()
        throws Exception
    {

        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "OutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        engine = new PayloadInputEngine("OutputInput", 0, "test");
        engine.registerComponentObserver(this);
        engine.start();

        PayloadReceiveChannel receiveChannel =
            engine.addDataChannel(sourceChannel, cacheMgr);

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after startup", engine.isRunning());

        testOutput = new PayloadOutputEngine("OutputInput", 0, "test");
        testOutput.registerComponentObserver(this);
        testOutput.start();

        PayloadTransmitChannel transmitEng =
            testOutput.addDataChannel(sinkChannel, cacheMgr);
        testOutput.registerStopNotificationCallback(SOURCE_NOTIFICATION_ID);
        testOutput.registerErrorNotificationCallback(SOURCE_NOTIFICATION_ID);

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", testOutput.isStopped());

        testOutput.startProcessing();
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after startup", testOutput.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int transmitCnt = 0;
        int recvCnt = 0;
        while (recvCnt < INPUT_OUTPUT_LOOP_CNT) {
            if (transmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                final int acquireLen = bufLen;
                testBuf = cacheMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              transmitCnt + " try.", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);
                transmitEng.flushOutQueue();
                transmitCnt++;
            }

            if (receiveChannel.inputQueue.isEmpty()) {
                Thread.sleep(100);
            } else {
                recvCnt++;
                ByteBuffer engineBuf =
                    (ByteBuffer)receiveChannel.inputQueue.take();
                assertEquals("Bad payload length", bufLen, engineBuf.getInt(0));
                assertEquals("Bad buffer position",
                             bufLen, engineBuf.position());
                // return the buffer for later use
                cacheMgr.returnBuffer(engineBuf);
            }

            if (transmitCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Nothing received after all buffers were transmitted");
            }
        }

        testOutput.sendLastAndStop();
        transmitEng.flushOutQueue();

        Thread.sleep(100);
        assertTrue("Failure on sendLastAndStop command.",
                   sourceStopNotificationCalled);

        Thread.sleep(100);
        assertTrue("Failure on sendLastAndStop command.",
                   sinkStopNotificationCalled);
    }

    public void testOutputInputWithSemaphore()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "OutInSem");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        engine = new PayloadInputEngine("OutInSemaphore", 0, "test");
        engine.registerComponentObserver(this);

        PayloadReceiveChannel receiveChannel =
            engine.addDataChannel(sourceChannel, cacheMgr);

        engine.start();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after startup", engine.isRunning());

        testOutput = new PayloadOutputEngine("OutInSemaphore", 0, "test");
        testOutput.registerComponentObserver(this);

        PayloadTransmitChannel transmitEng =
            testOutput.addDataChannel(sinkChannel, cacheMgr);
        testOutput.registerStopNotificationCallback(SOURCE_NOTIFICATION_ID);
        testOutput.registerErrorNotificationCallback(SOURCE_NOTIFICATION_ID);

        testOutput.start();

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", testOutput.isStopped());

        testOutput.startProcessing();
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after startup", testOutput.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 1024;

        int transmitCnt = 0;
        int recvCnt = 0;
        while (recvCnt < INPUT_OUTPUT_LOOP_CNT) {
            if (transmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                testBuf = cacheMgr.acquireBuffer(BUFFER_BLEN);
                assertNotNull("Unable to acquire transmit buffer on " +
                              transmitCnt + " try.", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(bufLen);
                testBuf.flip();

                sinkChannel.write(testBuf);
                transmitEng.flushOutQueue();
                transmitCnt++;
            }

            if (engine.inputAvailable.permits() == 0) {
                Thread.sleep(100);
            } else {
                engine.inputAvailable.acquire();

                recvCnt++;

                ByteBuffer engineBuf =
                    (ByteBuffer)receiveChannel.inputQueue.take();
                assertEquals("Bad payload length",
                             bufLen, engineBuf.getInt(0));
                assertEquals("Bad buffer position",
                             bufLen, engineBuf.position());

                // return the buffer for later use
                cacheMgr.returnBuffer(engineBuf);
            }

            if (transmitCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Nothing received after all buffers were transmitted");
            }
        }

        testOutput.sendLastAndStop();
        transmitEng.flushOutQueue();

        Thread.sleep(100);

        assertTrue("Failure on sendLastAndStop command.",
                   sourceStopNotificationCalled);

        Thread.sleep(100);

        assertTrue("Failure on sendLastAndStop command.",
                   sinkStopNotificationCalled);

        assertEquals("Failure on sendLastAndStop command.",
                     0, engine.inputAvailable.permits());
    }

    public void testSimulatedError()
        throws Exception
    {

        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "SimError");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        engine = new PayloadInputEngine("SimError", 0, "test");
        engine.registerComponentObserver(this);

        PayloadReceiveChannel receiveChannel =
            engine.addDataChannel(sourceChannel, cacheMgr);

        engine.start();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after startup", engine.isRunning());


        testOutput = new PayloadOutputEngine("SimErrorOut", 0, "test");
        testOutput.registerComponentObserver(this);

        PayloadTransmitChannel transmitEng =
            testOutput.addDataChannel(sinkChannel, cacheMgr);
        testOutput.registerStopNotificationCallback(SOURCE_NOTIFICATION_ID);
        testOutput.registerErrorNotificationCallback(SOURCE_NOTIFICATION_ID);

        testOutput.start();

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", testOutput.isStopped());

        testOutput.startProcessing();
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after startup", testOutput.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        final int acquireLen = bufLen;
        testBuf = cacheMgr.acquireBuffer(acquireLen);
        assertNotNull("Unable to acquire transmit buffer", testBuf);

        testBuf.putInt(0, bufLen);
        testBuf.limit(bufLen);
        testBuf.position(bufLen);
        testBuf.flip();

        engine.injectError();
        sinkChannel.write(testBuf);
        transmitEng.flushOutQueue();
        for (int i = 0; !engine.isError() && i < 10; i++) {
            Thread.sleep(100);
        }

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Error after ErrorSig", engine.isError());
        Thread.sleep(100);

        assertTrue("Error notification was not received.",
                   sinkErrorNotificationCalled);

        testOutput.sendLastAndStop();
        transmitEng.flushOutQueue();

        Thread.sleep(100);
        assertTrue("Source stop notification was not received.",
                   sourceStopNotificationCalled);

        Thread.sleep(100);
        assertFalse("Sink stop notification was received.",
                    sinkStopNotificationCalled);
    }

    public void testGetters()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "Getters");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        engine = new PayloadInputEngine("Getters", 0, "test");
        engine.registerComponentObserver(this);

        PayloadReceiveChannel receiveChannel =
            engine.addDataChannel(sourceChannel, cacheMgr);

        engine.start();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after startup", engine.isRunning());


        testOutput = new PayloadOutputEngine("GettersOut", 0, "test");
        testOutput.start();

        testOutput.registerComponentObserver(this);

        PayloadTransmitChannel transmitEng =
            testOutput.addDataChannel(sinkChannel, cacheMgr);
        testOutput.registerStopNotificationCallback(SOURCE_NOTIFICATION_ID);
        testOutput.registerErrorNotificationCallback(SOURCE_NOTIFICATION_ID);

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", testOutput.isStopped());

        testOutput.startProcessing();
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after startup", testOutput.isRunning());

        checkGetters(engine, 1, 0, 0, 0, 0, 0);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        final int acquireLen = bufLen;
        //testBuf = cacheMgr.acquireBuffer(acquireLen);
        testBuf = ByteBuffer.allocateDirect(acquireLen);
        assertNotNull("Unable to acquire transmit buffer", testBuf);

        testBuf.putInt(0, bufLen);
        testBuf.limit(bufLen);
        testBuf.position(bufLen);
        testBuf.flip();

        sinkChannel.write(testBuf);
        transmitEng.flushOutQueue();
        for (int i = 0; !engine.isError() && i < 10; i++) {
            Thread.sleep(100);
        }

        checkGetters(engine, 1, 1, BUFFER_BLEN, bufLen, 1, 0);

        assertFalse("PayloadInputEngine in Error state after ErrorSig",
                   engine.isError());
        assertFalse("Error notification was received.",
                   sinkErrorNotificationCalled);

        testOutput.sendLastAndStop();
        transmitEng.flushOutQueue();

        for (int i = 0; i < 5 && !engine.isStopped(); i++) {
            Thread.sleep(100);
        }

        assertTrue("Source stop notification was not received.",
                   sourceStopNotificationCalled);
        assertTrue("Sink stop notification was not received.",
                   sinkStopNotificationCalled);

        checkGetters(engine, 0, 0, BUFFER_BLEN, bufLen, 1, 1);
    }

    /**
     * Test starting and stopping server version of input engine.
     */
    public void testInetServer()
        throws Exception
    {
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "InetServer");

        engine = new PayloadInputEngine("InetServer", 0, "test");
        engine.start();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startServer(cacheMgr);

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after server start", engine.isStopped());

        InetSocketAddress addr =
            new InetSocketAddress("localhost", engine.getServerPort());
        SocketChannel chan = SocketChannel.open(addr);

        Thread.sleep(100);

        // this socket will die in the middle of the show
        SocketChannel redShirt = SocketChannel.open(addr);

        Thread.sleep(100);

        engine.startProcessing();

        ByteBuffer testBuf;

        final int bufLen = 64;

        int transmitCnt = 0;
        int recvCnt = 0;
        while (transmitCnt < INPUT_OUTPUT_LOOP_CNT) {
            final int acquireLen = bufLen;
            testBuf = cacheMgr.acquireBuffer(acquireLen);
            assertNotNull("Unable to acquire transmit buffer on " +
                          transmitCnt + " try.", testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chan.write(testBuf);
            if (transmitCnt < 2) {
                testBuf.position(0);
                redShirt.write(testBuf);
            } else {
                redShirt.close();
            }

            transmitCnt++;
            Thread.sleep(100);
        }

        engine.forcedStopProcessing();
        Thread.sleep(100);

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());

        // try it a second time
        engine.startProcessing();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        engine.forcedStopProcessing();
        Thread.sleep(100);

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());


        engine.destroyProcessor();

        for (int i = 0; i < 5 && !engine.isDestroyed(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadInputEngine did not die after kill request",
                   engine.isDestroyed());

        try {
            engine.startProcessing();
            fail("PayloadInputEngine restart after kill succeeded");
        } catch (Exception e) {
            // expect this to fail
        }
    }

    /**
     * Test multiple input engine servers.
     */
    public void testMultiServer()
        throws Exception
    {
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "MultiServer");

        final int numEngines = 4;

        PayloadInputEngine[] engines = new PayloadInputEngine[numEngines];

        // create a bunch of engines
        for (int i = 0; i < numEngines; i++) {
            engines[i] = new PayloadInputEngine("MultiServer", i, "test");
            engines[i].start();

            assertTrue("PayloadInputEngine in " + engines[i].getPresentState() +
                       ", not Idle after creation", engines[i].isStopped());
        }

        // start all the servers
        for (int i = 0; i < numEngines; i++) {
            engines[i].startServer(cacheMgr);

            assertTrue("PayloadInputEngine in " + engines[i].getPresentState() +
                   ", not Idle after server start", engines[i].isStopped());
        }

        SocketChannel[] chans = new SocketChannel[numEngines];

        // open a channel to each engine
        for (int i = 0; i < numEngines; i++) {
            InetSocketAddress addr =
                new InetSocketAddress("localhost", engines[i].getServerPort());
            chans[i] = SocketChannel.open(addr);
        }

        Thread.sleep(100);

        // gentlemen, start your engines
        for (int i = 0; i < numEngines; i++) {
            engines[i].startProcessing();
        }

        Thread.sleep(100);

        // make sure they're all running
        for (int i = 0; i < numEngines; i++) {
            assertTrue("PayloadInputEngine in " + engines[i].getPresentState() +
                       ", not Running after server start",
                       engines[i].isRunning());
        }

        ByteBuffer testBuf;

        final int bufLen = 64;

        for (int i = 0; i < numEngines; i++) {
            testBuf = cacheMgr.acquireBuffer(bufLen);
            assertNotNull("Unable to acquire transmit buffer#" + i, testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chans[i].write(testBuf);
        }

        Thread.sleep(100);

        boolean gotAll = false;

        // wait until we've got data on all channels
        final int numTries = 5;
        for (int i = 0; !gotAll && i < numTries; i++) {
            boolean rcvdData = true;
            for (int j = 0; rcvdData && j < numEngines; j++) {
                Long[] rcvd = engines[j].getBytesReceived();
                assertNotNull("Got null byteRcvd array from engine#" + j,
                              rcvd);
                assertEquals("Unexpected number of connections for engine#" + j,
                             1, rcvd.length);
                if (rcvd[0].longValue() < bufLen) {
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
        for (int i = 0; i < numEngines; i++) {
            engines[i].forcedStopProcessing();
        }

        // destroy everything
        for (int i = 0; i < numEngines; i++) {
            engines[i].destroyProcessor();
        }

        for (int i = 0; i < numEngines; i++) {
            assertTrue("PayloadInputEngine#" + i +
                       " did not die after kill request",
                       engines[i].isDestroyed());
        }
    }

    public void testServerInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "ServerInput");

        Selector sel = Selector.open();

        int port = createServer(sel);

        engine = new PayloadInputEngine("ServerInput", 0, "test");
        engine.start();

        engine.addReverseConnection("localhost", port, cacheMgr);
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after start", engine.isStopped());

        ByteBuffer testBuf;

        for (int i = 0; i < 2; i++) {
            engine.startProcessing();
            assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                       ", not Running after StartSig", engine.isRunning());

            SocketChannel chan = acceptChannel(sel);

            final int bufLen = 40;

            testBuf = cacheMgr.acquireBuffer(bufLen);
            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chan.write(testBuf);

            // wait until we've got data on all channels
            boolean gotAll = false;
            final int numTries = 5;
            for (int t = 0; !gotAll && t < numTries; t++) {
                boolean rcvdData = true;
                Long[] rcvd = engine.getBytesReceived();
                assertNotNull("Got null byteRcvd array from engine", rcvd);
                assertEquals("Unexpected number of connections for engine",
                             1, rcvd.length);
                if (rcvd[0].longValue() < bufLen) {
                    rcvdData = false;
                }

                // if we've got data, we're done
                if (rcvdData) {
                    gotAll = true;
                } else {
                    Thread.sleep(100);
                }
            }

            Long[] totRcvd = engine.getBytesReceived();
            assertNotNull("Got null byteRcvd array from engine", totRcvd);
            assertEquals("Unexpected number of connections for engine",
                         1, totRcvd.length);
            assertEquals("Bad number of bytes", bufLen, totRcvd[0].longValue());

            testBuf = cacheMgr.acquireBuffer(4);
            assertNotNull("Unable to acquire stop buffer", testBuf);

            testBuf.putInt(4);
            testBuf.limit(4);
            testBuf.position(4);
            testBuf.flip();

            chan.write(testBuf);

            int reps = 0;
            while (reps < 10 && !engine.isStopped()) {
                Thread.sleep(10);
                reps++;
            }
            if (reps >= 10) {
                fail("Engine did not stop after receiving stop msg");
            }

            // make sure receive engines have been detatched
            Long[] postRcvd = engine.getBytesReceived();
            assertNotNull("Got null byteRcvd array from engine", postRcvd);
            assertEquals("Unexpected number of connections for engine",
                         0, postRcvd.length);
        }

        engine.destroyProcessor();
        assertTrue("PayloadInputEngine did not die after kill request",
                   engine.isDestroyed());
    }

    public synchronized void update(Object object, String notificationID)
    {
        if (object instanceof NormalState){
            NormalState state = (NormalState)object;
            if (state == NormalState.STOPPED){
                if (notificationID.equals(DAQCmdInterface.SINK)){
                    sinkStopNotificationCalled = true;
                } else if (notificationID.equals(DAQCmdInterface.SOURCE)){
                    sourceStopNotificationCalled = true;
                }
            }
        } else if (object instanceof ErrorState){
            ErrorState state = (ErrorState)object;
            if (state == ErrorState.UNKNOWN_ERROR){
                if (notificationID.equals(DAQCmdInterface.SINK)){
                    sinkErrorNotificationCalled = true;
                } else if (notificationID.equals(DAQCmdInterface.SOURCE)){
                    sourceErrorNotificationCalled = true;
                }
            }
        }
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
