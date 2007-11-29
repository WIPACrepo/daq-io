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

class MockPushSimpleRdr
    extends PushSimpleReader
{
    private IByteBufferCache bufMgr;
    private int recvCnt;
    private boolean gotStop;

    MockPushSimpleRdr(String name, IByteBufferCache bufMgr)
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

public class PushSimpleReaderTest
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

    private MockPushSimpleRdr tstRdr;

    /**
     * Construct an instance of this test.
     *
     * @param name the name of the test.
     */
    public PushSimpleReaderTest(String name)
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
        return new TestSuite(PushSimpleReaderTest.class);
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

        tstRdr = new MockPushSimpleRdr("StartStop", bufMgr);

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

        tstRdr.forcedStopProcessing();
        waitUntilStopped(tstRdr, "forced stop");

        tstRdr.destroyProcessor();
        waitUntilDestroyed(tstRdr);
        assertTrue("Reader did not die after kill request",
                   tstRdr.isDestroyed());

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            tstRdr.startProcessing();
            fail("Reader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }
    }

    public void testStartDispose()
        throws Exception
    {
        IByteBufferCache bufMgr = new VitreousBufferCache();

        tstRdr = new MockPushSimpleRdr("StartDisp", bufMgr);

        tstRdr.start();
        waitUntilStopped(tstRdr, "creation");

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);

        tstRdr.startDisposing();
        waitUntilDisposing(tstRdr);
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

        tstRdr = new MockPushSimpleRdr("OutIn", bufMgr);
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
        waitUntilStopped(tstRdr, "stop msg");
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

        tstRdr = new MockPushSimpleRdr("MultiOutIn", bufMgr);
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
        waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Failure on sendStopMsg command.", observer.gotSinkStop());
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
