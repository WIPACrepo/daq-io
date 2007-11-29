package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockSpliceableFactory;
import icecube.daq.io.test.MockSplicer;
import icecube.daq.io.test.MockStrandTail;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.VitreousBufferCache;

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
import junit.framework.TestSuite;

import junit.textui.TestRunner;

class SimpleObserver
    implements DAQComponentObserver
{
    private boolean sinkStopNotificationCalled;
    private boolean sinkErrorNotificationCalled;

    boolean gotError()
    {
        return sinkErrorNotificationCalled;
    }

    boolean gotStop()
    {
        return sinkStopNotificationCalled;
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
}

public class SpliceableSimpleReaderTest
    extends LoggingCase
{
    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

    private static ByteBuffer stopMsg;

    private SpliceableSimpleReader tstRdr;

    /**
     * Construct an instance of this test.
     *
     * @param name the name of the test.
     */
    public SpliceableSimpleReaderTest(String name)
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
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(SpliceableSimpleReaderTest.class);
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

        tstRdr = new SpliceableSimpleReader("StartStop", splicer, factory);

        tstRdr.start();
        waitUntilStopped(tstRdr, "creation");

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);

        tstRdr.forcedStopProcessing();
        waitUntilStopped(tstRdr, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // try it a second time
        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);

        assertEquals("Bad number of log messages",
                     1, getNumberOfMessages());
        assertEquals("Unexpected log message 0",
                     "Splicer should have been in STOPPED state," +
                     " not MockState.  Calling Splicer.forceStop()",
                     getMessage(0));
        clearMessages();

        tstRdr.forcedStopProcessing();
        waitUntilStopped(tstRdr, "forced stop");

        tstRdr.destroyProcessor();
        waitUntilDestroyed(tstRdr);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            tstRdr.startProcessing();
            fail("Reader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }

        assertEquals("Bad number of log messages",
                     1, getNumberOfMessages());
        assertEquals("Unexpected log message 0",
                     "Splicer should have been in STOPPED state," +
                     " not MockState.  Calling Splicer.forceStop()",
                     getMessage(0));
        clearMessages();
    }

    public void testOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new VitreousBufferCache();

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockSplicer splicer = new MockSplicer();
        MockSpliceableFactory factory = new MockSpliceableFactory();

        SimpleObserver observer = new SimpleObserver();

        tstRdr = new SpliceableSimpleReader("OutputInput", splicer, factory);
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        Thread.sleep(100);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);

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
        assertTrue("Observer didn't see stop.", observer.gotStop());
    }

    public void testMultiOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new VitreousBufferCache();

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockSplicer splicer = new MockSplicer();
        MockSpliceableFactory factory = new MockSpliceableFactory();

        SimpleObserver observer = new SimpleObserver();

        tstRdr = new SpliceableSimpleReader("OutputInput", splicer, factory);
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        Thread.sleep(100);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);

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
        waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see stop.", observer.gotStop());
    }

    private static final void waitUntilDestroyed(SimpleReader rdr)
    {
        waitUntilDestroyed(rdr, "");
    }

    private static final void waitUntilDestroyed(SimpleReader rdr,
                                                 String extra)
    {
        for (int i = 0; i < 5 && !rdr.isDestroyed(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("Reader did not die after kill request" + extra,
                   rdr.isDestroyed());
    }

    private static final void waitUntilDisposing(SimpleReader rdr)
    {
        waitUntilDisposing(rdr, "");
    }

    private static final void waitUntilDisposing(SimpleReader rdr,
                                                 String extra)
    {
        for (int i = 0; i < 5 && !rdr.isDisposing(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("Reader in " + rdr.getPresentState() +
                   ", not Disposing after DisposeSig" + extra,
                   rdr.isDisposing());
    }

    private static final void waitUntilRunning(SimpleReader rdr)
    {
        waitUntilRunning(rdr, "");
    }

    private static final void waitUntilRunning(SimpleReader rdr, String extra)
    {
        for (int i = 0; i < 5 && !rdr.isRunning(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("Reader in " + rdr.getPresentState() +
                   ", not Running after StartSig" + extra, rdr.isRunning());
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

    private static final void waitUntilStopped(SimpleReader rdr, String action)
    {
        waitUntilStopped(rdr, action, "");
    }

    private static final void waitUntilStopped(SimpleReader rdr, String action,
                                               String extra)
    {
        for (int i = 0; i < 5 && !rdr.isStopped(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("Reader in " + rdr.getPresentState() +
                   ", not Idle after " + action + extra, rdr.isStopped());
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
