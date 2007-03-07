package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.ErrorState;
import icecube.daq.common.NormalState;

import icecube.daq.io.test.MockAppender;
import icecube.daq.io.test.MockSpliceableFactory;
import icecube.daq.io.test.MockSplicer;
import icecube.daq.io.test.MockStrandTail;

import icecube.daq.payload.ByteBufferCache;
import icecube.daq.payload.IByteBufferCache;

import icecube.daq.splicer.Splicer;

import java.io.IOException;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;

import java.nio.channels.Pipe;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

public class SpliceablePayloadReaderTest
    extends TestCase
    implements DAQComponentObserver
{
    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

    private static Level logLevel = Level.INFO;

    private static ByteBuffer stopMsg;

    private boolean sinkStopNotificationCalled;
    private boolean sinkErrorNotificationCalled;

    private SpliceablePayloadReader tstRdr;

    /**
     * Construct an instance of this test.
     *
     * @param name the name of the test.
     */
    public SpliceablePayloadReaderTest(String name)
    {
        super(name);
    }

    private static final int harvestStrands(MockSplicer splicer,
                                            IByteBufferCache bufMgr)
    {
        int numReturned = 0;
        for (Iterator tIter = splicer.iterator(); tIter.hasNext(); ) {
            numReturned +=
                ((MockStrandTail) tIter.next()).returnBuffers(bufMgr);
        }

        return numReturned;
    }

    private static final void sendStopMsg(WritableByteChannel sinkChannel)
        throws IOException
    {
        if (stopMsg == null) {
            stopMsg = ByteBuffer.allocate(4);
            stopMsg.putInt(0, 4);
            stopMsg.limit(4);
        }

        stopMsg.position(0);
        sinkChannel.write(stopMsg);
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        tstRdr = null;

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
        return new TestSuite(SpliceablePayloadReaderTest.class);
    }

    protected void tearDown()
        throws Exception
    {
        if (tstRdr != null) {
            tstRdr.destroyProcessor();
        }

        super.tearDown();
    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        MockSplicer splicer = new MockSplicer();
        MockSpliceableFactory factory = new MockSpliceableFactory();

        tstRdr = new SpliceablePayloadReader("StartStop", splicer, factory);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        tstRdr.forcedStopProcessing();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after StopSig", tstRdr.isStopped());

        // try it a second time
        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        tstRdr.forcedStopProcessing();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after StopSig", tstRdr.isStopped());

        tstRdr.destroyProcessor();
        waitUntilDestroyed(tstRdr);
        assertTrue("PayloadReader did not die after kill request",
                   tstRdr.isDestroyed());

        try {
            tstRdr.startProcessing();
            fail("PayloadReader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }
    }

    public void testOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "OutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockSplicer splicer = new MockSplicer();
        MockSpliceableFactory factory = new MockSpliceableFactory();

        tstRdr = new SpliceablePayloadReader("OutputInput", splicer, factory);
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        Thread.sleep(100);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int loopCnt = 0;
        while (tstRdr.getTotalStrandDepth() < INPUT_OUTPUT_LOOP_CNT) {
            if (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                final int acquireLen = bufLen;
                testBuf = bufMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try.", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);

                bufMgr.returnBuffer(testBuf);
                xmitCnt++;
            } else {
                try {
                    Thread.sleep(100);
                } catch (Exception ex) {
                    // ignore interrupts
                }
            }

            loopCnt++;
            if (loopCnt > INPUT_OUTPUT_LOOP_CNT * 2) {
                fail("Received " + tstRdr.getTotalStrandDepth() +
                     " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertTrue("Failure on sendStopMsg command.",
                   sinkStopNotificationCalled);
    }

    public void testMultiOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "MultiOutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockSplicer splicer = new MockSplicer();
        MockSpliceableFactory factory = new MockSpliceableFactory();

        tstRdr = new SpliceablePayloadReader("OutputInput", splicer, factory);
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        Thread.sleep(100);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

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
                              xmitCnt + " try.", testBuf);

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

            int numBufs = harvestStrands(splicer, bufMgr);
            if (numBufs > 0) {
                recvId += numBufs;
                recvCnt += numBufs;
            }

            loopCnt++;
            if (loopCnt > numToSend * 2) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertTrue("Failure on sendStopMsg command.",
                   sinkStopNotificationCalled);

        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after stop", tstRdr.isStopped());
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

    private static final void waitUntilDestroyed(PayloadReader rdr)
    {
        for (int i = 0; i < 5 && !rdr.isDestroyed(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }
    }

    private static final void waitUntilRunning(PayloadReader rdr)
    {
        for (int i = 0; i < 5 && !rdr.isRunning(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }
    }

    private static final void waitUntilStopped(PayloadReader rdr)
    {
        for (int i = 0; i < 5 && !rdr.isStopped(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
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
