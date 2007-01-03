/*
 * class: PayloadOutputEngineTest
 *
 * Version $Id: PayloadOutputEngineTest.java,v 1.4 2006/06/30 18:07:06 dwharton Exp $
 *
 * Date: May 19 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io.test;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import icecube.daq.common.*;
import icecube.daq.io.PayloadOutputEngine;
import icecube.daq.io.PayloadTransmitChannel;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ByteBufferCache;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.Pipe;
import java.nio.ByteBuffer;
import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * This class defines the tests that any PayloadOutputEngine object should pass.
 *
 * @author mcp
 * @version $Id: PayloadOutputEngineTest.java,v 1.4 2006/06/30 18:07:06 dwharton Exp $
 */
public class PayloadOutputEngineTest
        extends TestCase implements DAQComponentObserver {

    // private static member data
    private static final int BUFFER_BLEN = 32000;
    private static final String SRC_NOTIFICATION_ID = "SourceID";
    private static final String SRC_ERROR_ID = "SourceErrorID";

    // private instance member data
    private boolean sourceStopNotificationCalled;
    private boolean sourceErrorNotificationCalled;
    private boolean sourceStopNotificationStatus;
    private String receivedStopNotificationID = "";
    private String receivedErrorNotificationID = "";

    /**
     * The object being tested.
     */
    private PayloadOutputEngine engine;

    /**
     * Constructs and instance of this test.
     *
     * @param name the name of the test.
     */
    public PayloadOutputEngineTest(String name)
    {
        super(name);
    }


    public synchronized void DAQComponentProcessStopNotification(String notificationID,
                                                                 boolean status) {
        receivedStopNotificationID = notificationID;
        if (notificationID.compareTo(SRC_NOTIFICATION_ID) == 0) {
            sourceStopNotificationCalled = true;
            sourceStopNotificationStatus = status;
        }
    }

    public synchronized void DAQComponentProcessErrorNotification(String notificationID,
                                                                  String info,
                                                                  Exception e) {
        receivedErrorNotificationID = notificationID;
        if (notificationID.compareTo(SRC_ERROR_ID) == 0) {
            sourceErrorNotificationCalled = true;
        }

        //System.out.println("Error notification.");
    }

    public void DAQConfigurationNotification(String info, boolean status) {

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

        sourceErrorNotificationCalled = false;
        sourceStopNotificationCalled = false;
        sourceStopNotificationStatus = false;
        receivedErrorNotificationID = "";
        receivedStopNotificationID = "";
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite() {
        return new TestSuite(PayloadOutputEngineTest.class);
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
        engine = new PayloadOutputEngine("StartStop", 0, "test");
        engine.start();

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after creation", engine.isStopped());

        engine.startProcessing();

        for (int i = 0; i < 5 && !engine.isRunning(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        engine.forcedStopProcessing();

        for (int i = 0; i < 5 && !engine.isStopped(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());

        // try it a second time
        engine.startProcessing();

        for (int i = 0; i < 5 && !engine.isRunning(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        engine.forcedStopProcessing();

        for (int i = 0; i < 5 && !engine.isStopped(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());

        // now try a stop message
        engine.startProcessing();

        for (int i = 0; i < 5 && !engine.isRunning(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        engine.sendLastAndStop();

        for (int i = 0; i < 5 && !engine.isStopped(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());

        engine.destroyProcessor();

        for (int i = 0; i < 5 && !engine.isDestroyed(); i++) {
            Thread.sleep(100);
        }
        assertTrue("PayloadOutputEngine did not die after kill request",
                   engine.isDestroyed());

        try {
            engine.startProcessing();
            fail("PayloadOutputEngine restart after kill succeeded");
        } catch (Exception e) {
            // expect this to fail
        }
    }

    public void testInjectError() throws Exception {
        engine = new PayloadOutputEngine("InjectError", 0, "test");
        engine.start();
        engine.registerComponentObserver(this);
        assertTrue("PayloadOutputEngine not in Idle state after creation", engine.isStopped());
        engine.startProcessing();


        // inject an error
        engine.injectError();
        Thread.sleep(500);

        // make sure it was found
        assertTrue("PayloadOutputEngine in " +
                   engine.getPresentState() +
                   ", not in Error after injecting error",
                   engine.isError());

        assertTrue("PayloadOutputEngine error notification not called",
                   sourceErrorNotificationCalled);
        assertFalse("PayloadOutputEngine stop notification incorrectly" +
                    " called", sourceStopNotificationCalled);
        assertEquals("PayloadOutputEngine error notification incorrect",
                     DAQCmdInterface.SOURCE,
                     receivedErrorNotificationID);
    }

    public void testInjectLowLevelError() throws Exception {

        engine = new PayloadOutputEngine("LowLevel", 0, "test");
        engine.start();
        engine.registerComponentObserver(this);

        assertTrue("PayloadOutputEngine not in Idle state after creation", engine.isStopped());
        engine.startProcessing();

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

        // inject an error
        engine.injectLowLevelError();
        Thread.sleep(500);

        // make sure it was found
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Error after injecting error",
                   engine.isError());

        assertTrue("PayloadOutputEngine error notification not called",
                   sourceErrorNotificationCalled);
        assertFalse("PayloadOutputEngine stop notification incorrectly called",
                    sourceStopNotificationCalled);
        assertEquals("PayloadOutputEngine error notification incorrect",
                     DAQCmdInterface.SOURCE,
                     receivedErrorNotificationID);

        engine.forcedStopProcessing();
        Thread.sleep(500);
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Error after StopSig", engine.isError());
    }

    public void testOutputLoop() throws Exception {
        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "OutputLoop");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        engine = new PayloadOutputEngine("OutputLoop", 0, "test");
        engine.registerComponentObserver(this);
        engine.start();
        PayloadTransmitChannel transmitEng =
            engine.addDataChannel(testPipe.sink(), cacheMgr);
        transmitEng.registerComponentObserver(this, "OutputLoop");
        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

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

            transmitEng.outputQueue.put(testOutBuf);
            transmitEng.flushOutQueue();
            for (int j = 0; j < 10; j++) {
                if (!transmitEng.outputQueue.isEmpty()) {
                    Thread.sleep(20);
                }
            }
            assertTrue("PayloadTransmitChannel did not send buf#" + i,
                       transmitEng.outputQueue.isEmpty());

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
        Thread.sleep(10);
        transmitEng.flushOutQueue();

        for (int i = 0; i < 5 && !engine.isStopped(); i++) {
            Thread.sleep(100);
        }
        assertTrue("Failure on sendLastAndStop command.",
                   sourceStopNotificationCalled);
    }

    public void testServerOutput() throws Exception {
        // buffer caching manager
        IByteBufferCache cacheMgr =
            new ByteBufferCache(BUFFER_BLEN, BUFFER_BLEN*20,
                                BUFFER_BLEN*40, "ServerOutput");

        Selector sel = Selector.open();

        int port = createServer(sel);

        engine = new PayloadOutputEngine("ServerOutput", 0, "test");
        engine.registerComponentObserver(this);
        engine.start();

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        PayloadTransmitChannel transmitEng = engine.connect(cacheMgr, sock, 1);
        transmitEng.registerComponentObserver(this, "ServerOutput");

        SocketChannel chan = acceptChannel(sel);

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();

        assertTrue("PayloadOutputEngine in " + engine.getPresentState() +
                   ", not Running after StartSig", engine.isRunning());

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

            transmitEng.receiveByteBuffer(testOutBuf);
            for (int j = 0; j < 10; j++) {
                if (!transmitEng.isOutputQueued()) {
                    break;
                }

                Thread.sleep(100);
            }
            assertFalse("PayloadTransmitChannel did not send buf#" + i,
                       transmitEng.isOutputQueued());

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
        transmitEng.flushOutQueue();
        Thread.sleep(10);
        assertTrue("Failure on sendLastAndStop command.",
                   sourceStopNotificationCalled);
    }

    public synchronized void update(Object object, String notificationID){
                if (object instanceof NormalState){
                    NormalState state = (NormalState)object;
                    if (state == NormalState.STOPPED){
                        if (notificationID.equals(DAQCmdInterface.SOURCE)){
                            sourceStopNotificationCalled = true;
                        }
                    }
                } else if (object instanceof ErrorState){
                    ErrorState state = (ErrorState)object;
                    if (state == ErrorState.UNKNOWN_ERROR){
                        if (notificationID.equals(DAQCmdInterface.SOURCE)){
                            sourceErrorNotificationCalled = true;
                            receivedErrorNotificationID = DAQCmdInterface.SOURCE;
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
