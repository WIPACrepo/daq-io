package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.io.test.LoggingCase;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.VitreousBufferCache;

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

class MockPushReader
    extends PushPayloadReader
{
    private IByteBufferCache bufMgr;
    private int recvCnt;
    private boolean gotStop;

    MockPushReader(String name, IByteBufferCache bufMgr)
        throws IOException
    {
        super(name);

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

public class PushPayloadReaderTest
    extends LoggingCase
{
    class Observer
        implements DAQComponentObserver
    {
        private String sinkNotificationId;
        private boolean sinkStopNotificationCalled;
        private boolean sinkErrorNotificationCalled;

        private String sourceNotificationId;
        private boolean sourceStopNotificationCalled;
        private boolean sourceErrorNotificationCalled;

        boolean gotSinkError()
        {
            return sinkErrorNotificationCalled;
        }

        boolean gotSinkStop()
        {
            return sinkStopNotificationCalled;
        }

        boolean gotSourceError()
        {
            return sourceErrorNotificationCalled;
        }

        boolean gotSourceStop()
        {
            return sourceStopNotificationCalled;
        }

        void setSinkNotificationId(String id)
        {
            sinkNotificationId = id;
        }

        void setSourceNotificationId(String id)
        {
            sourceNotificationId = id;
        }

        public synchronized void update(Object object, String notificationId)
        {
            if (object instanceof NormalState) {
                NormalState state = (NormalState)object;
                if (state == NormalState.STOPPED) {
                    if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                        notificationId.equals(sourceNotificationId))
                    {
                        sourceStopNotificationCalled = true;
                    } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                               notificationId.equals(sinkNotificationId))
                    {
                        sinkStopNotificationCalled = true;
                    } else {
                        throw new Error("Unexpected stop notification \"" +
                                        notificationId + "\"");
                    }
                } else {
                    throw new Error("Unexpected notification state " +
                                    state);
                }
            } else if (object instanceof ErrorState) {
                ErrorState state = (ErrorState)object;
                if (state == ErrorState.UNKNOWN_ERROR) {
                    if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                        notificationId.equals(sourceNotificationId))
                    {
                        sourceErrorNotificationCalled = true;
                    } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                               notificationId.equals(sinkNotificationId))
                    {
                        sourceStopNotificationCalled = true;
                    } else {
                        throw new Error("Unexpected error notification \"" +
                                        notificationId + "\"");
                    }
                } else {
                    throw new Error("Unexpected notification state " +
                                    state);
                }
            } else {
                throw new Error("Unexpected notification object " +
                                object.getClass().getName());
            }
        }
    }

    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

    private static ByteBuffer stopMsg;

    private MockPushReader tstRdr;

    /**
     * Construct an instance of this test.
     *
     * @param name the name of the test.
     */
    public PushPayloadReaderTest(String name)
    {
        super(name);
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
        return new TestSuite(PushPayloadReaderTest.class);
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
        IByteBufferCache bufMgr = new VitreousBufferCache();

        tstRdr = new MockPushReader("StartStop", bufMgr);

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

    public void testStartDispose()
        throws Exception
    {
        IByteBufferCache bufMgr = new VitreousBufferCache();

        tstRdr = new MockPushReader("StartDisp", bufMgr);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        tstRdr.startDisposing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartDisposing",
                   tstRdr.isRunning());
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

        Observer observer = new Observer();

        tstRdr = new MockPushReader("OutIn", bufMgr);
        tstRdr.registerComponentObserver(observer);

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
        while (tstRdr.getReceiveCount() < INPUT_OUTPUT_LOOP_CNT) {
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
                fail("Received " + tstRdr.getReceiveCount() +
                     " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertTrue("Failure on sendStopMsg command.", observer.gotSinkStop());
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

        Observer observer = new Observer();

        tstRdr = new MockPushReader("MultiOutIn", bufMgr);
        tstRdr.registerComponentObserver(observer);

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
        int loopCnt = 0;
        while (tstRdr.getReceiveCount() < numToSend) {
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

            loopCnt++;
            if (loopCnt == numToSend * 2) {
                fail("Received " + tstRdr.getReceiveCount() +
                     " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertTrue("Failure on sendStopMsg command.", observer.gotSinkStop());

        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after stop", tstRdr.isStopped());
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
