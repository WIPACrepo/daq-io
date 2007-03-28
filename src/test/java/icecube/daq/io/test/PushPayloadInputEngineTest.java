package icecube.daq.io.test;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.ErrorState;
import icecube.daq.common.NormalState;

import icecube.daq.io.PushPayloadInputEngine;
import icecube.daq.io.PayloadOutputEngine;
import icecube.daq.io.PayloadTransmitChannel;
import icecube.daq.io.PayloadReceiveChannel;

import icecube.daq.payload.ByteBufferCache;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;

import java.nio.channels.Pipe;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

class MockPushEngine
    extends PushPayloadInputEngine
{
    private IByteBufferCache bufMgr;
    private int recvCnt;
    private boolean gotStop;

    MockPushEngine(String name, int id, String fcn, String prefix,
                   IByteBufferCache bufMgr)
    {
        super(name, id, fcn, prefix, bufMgr);

        this.bufMgr = bufMgr;
    }

    int getReceiveCount()
    {
        return recvCnt;
    }

    public void pushBuffer(ByteBuffer bb)
        throws IOException
    {
        bufMgr.returnBuffer(bb);
        recvCnt++;
    }

    public void sendStop()
    {
        gotStop = true;
    }
}

public class PushPayloadInputEngineTest
    extends TestCase
    implements DAQComponentObserver
{
    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

    private static final String SRC_NOTE_ID = "SourceID";
    private static final String ERR_NOTE_ID = "ErrorID";

    private static Level logLevel = Level.INFO;

    private boolean sinkStopNotificationCalled;
    private boolean sinkErrorNotificationCalled;

    private MockPushEngine engine;
    private PayloadOutputEngine testOutput;

    /**
     * Construct an instance of this test.
     *
     * @param name the name of the test.
     */
    public PushPayloadInputEngineTest(String name)
    {
        super(name);
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        engine = null;

        sinkStopNotificationCalled = false;
        sinkErrorNotificationCalled = false;

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
        return new TestSuite(PushPayloadInputEngineTest.class);
    }

    protected void tearDown()
        throws Exception
    {
        if (engine != null) {
            engine.destroyProcessor();
        }

        super.tearDown();
    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "OutputInput");

        engine =
            new MockPushEngine("StartStop", 0, "test", "StStop", cacheMgr);
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

    public void testStartDispose()
        throws Exception
    {
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "OutputInput");

        engine =
            new MockPushEngine("StartDisp", 0, "test", "StDisp", cacheMgr);
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
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "OutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        engine = new MockPushEngine("OutIn", 0, "test", "OutIn", cacheMgr);
        engine.registerComponentObserver(this);
        engine.start();

        engine.addDataChannel(sourceChannel, cacheMgr);

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after startup", engine.isRunning());
        assertTrue("Should be healthy", engine.isHealthy());

        testOutput = new PayloadOutputEngine("OutputInput", 0, "test");
        testOutput.start();

        PayloadTransmitChannel transmitEng =
            testOutput.addDataChannel(sinkChannel, cacheMgr);

        assertTrue("PayloadOutputEngine in " + testOutput.getPresentState() +
                   ", not Idle after creation", testOutput.isStopped());

        testOutput.startProcessing();
        assertTrue("PayloadOutputEngine in " + testOutput.getPresentState() +
                   ", not Running after startup", testOutput.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        while (engine.getReceiveCount() < INPUT_OUTPUT_LOOP_CNT) {
            if (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                final int acquireLen = bufLen;
                testBuf = cacheMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try.", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);
                transmitEng.flushOutQueue();
            }
            xmitCnt++;

            if (xmitCnt > INPUT_OUTPUT_LOOP_CNT * 2) {
                break;
            }

            if (xmitCnt != engine.getReceiveCount()) {
                Thread.sleep(100);
            }
        }

        assertEquals("Bad number of payloads received",
                     xmitCnt, engine.getReceiveCount());

        assertTrue("Should be healthy", engine.isHealthy());

        testOutput.sendLastAndStop();
        transmitEng.flushOutQueue();

        Thread.sleep(100);
        assertTrue("Failure on sendLastAndStop command.",
                   sinkStopNotificationCalled);
    }

    public void testMultiOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "MultiOutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        engine =
            new MockPushEngine("MultiOutIn", 0, "test", "MOutIn", cacheMgr);
        engine.registerComponentObserver(this);
        engine.start();

        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        testOutput = new PayloadOutputEngine("MultiOutputInput", 0, "test");
        testOutput.start();

        assertTrue("PayloadOutputEngine in " + testOutput.getPresentState() +
                   ", not Idle after creation", testOutput.isStopped());

        engine.addDataChannel(sourceChannel, cacheMgr);

        engine.startProcessing();
        assertTrue("PayloadInputEngine in " + engine.getPresentState() +
                   ", not Running after startup", engine.isRunning());

        PayloadTransmitChannel transmitEng =
            testOutput.addDataChannel(sinkChannel, cacheMgr);

        testOutput.startProcessing();
        assertTrue("PayloadOutputEngine in " + testOutput.getPresentState() +
                   ", not Running after startup", testOutput.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;
        final int groupSize = 3;

        int id = 1;
        int recvId = 0;

        int xmitCnt = 0;
        while (engine.getReceiveCount() <
               (INPUT_OUTPUT_LOOP_CNT * groupSize))
        {
            if (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                final int acquireLen = bufLen * groupSize;
                testBuf = cacheMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try.", testBuf);

                for (int i = 0; i < groupSize; i++) {
                    final int start = bufLen * i;
                    testBuf.putInt(start, bufLen);
                    testBuf.putInt(start + 4, id++);
                }
                testBuf.limit(acquireLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);
                transmitEng.flushOutQueue();
                cacheMgr.returnBuffer(testBuf);
            }
            xmitCnt++;

            if (xmitCnt > INPUT_OUTPUT_LOOP_CNT * 2) {
                break;
            }

            if (xmitCnt * groupSize  != engine.getReceiveCount()) {
                Thread.sleep(100);
            }
        }

        assertEquals("Bad number of payloads received",
                     xmitCnt * groupSize, engine.getReceiveCount());

        testOutput.sendLastAndStop();
        transmitEng.flushOutQueue();

        Thread.sleep(100);
        assertTrue("Failure on sendLastAndStop command.",
                   sinkStopNotificationCalled);
    }

    public synchronized void update(Object object, String notificationID)
    {
        if (object instanceof NormalState){
            NormalState state = (NormalState)object;
            if (state == NormalState.STOPPED){
                if (notificationID.equals(DAQCmdInterface.SINK)){
                    sinkStopNotificationCalled = true;
                } else {
                    throw new Error("Unexpected notification update");
                }
            }
        } else if (object instanceof ErrorState){
            ErrorState state = (ErrorState)object;
            if (state == ErrorState.UNKNOWN_ERROR){
                if (notificationID.equals(DAQCmdInterface.SINK)){
                    sinkErrorNotificationCalled = true;
                } else {
                    throw new Error("Unexpected notification update");
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
