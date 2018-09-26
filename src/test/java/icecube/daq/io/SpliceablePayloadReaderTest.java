package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.test.IOTestUtil;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.io.test.MockObserver;
import icecube.daq.io.test.MockSpliceableFactory;
import icecube.daq.io.test.MockSplicer;
import icecube.daq.io.test.MockStrandTail;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.splicer.Splicer;

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

public class SpliceablePayloadReaderTest
    extends LoggingCase
{
    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

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

    @Override
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
        return new TestSuite(SpliceablePayloadReaderTest.class);
    }

    @Override
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
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        assertNoLogMessages();

        // try it a second time
        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        assertLogMessage("Splicer should have been in STOPPED state, not" +
                         " STARTED.  Calling Splicer.forceStop()");
        assertNoLogMessages();

        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        tstRdr.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(tstRdr);

        assertNoLogMessages();

        try {
            tstRdr.startProcessing();
            fail("Reader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }

        assertLogMessage("Splicer should have been in STOPPED state, not" +
                         " STARTED.  Calling Splicer.forceStop()");
        assertNoLogMessages();
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

        MockSplicer splicer = new MockSplicer();
        MockSpliceableFactory factory = new MockSpliceableFactory();

        MockObserver observer = new MockObserver("OutIn");

        tstRdr = new SpliceablePayloadReader("OutputInput", splicer, factory);
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, "OutInChan", bufMgr, 1024);

        Thread.sleep(100);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

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

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop.", observer.gotSinkStop());
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

        MockSplicer splicer = new MockSplicer();
        MockSpliceableFactory factory = new MockSpliceableFactory();

        MockObserver observer = new MockObserver("MultiOutIn");

        tstRdr = new SpliceablePayloadReader("OutputInput", splicer, factory);
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, "MultiOIChan", bufMgr, 1024);

        Thread.sleep(100);

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

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop.", observer.gotSinkStop());
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
