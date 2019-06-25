package icecube.daq.io;

import icecube.daq.io.test.MockBufferCache;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static org.junit.Assert.*;

/**
 * Tests BlockingOutputEngine.java
 */
public class BlockingOutputEngineTest
{
    private BlockingOutputEngine engine;
    private MockBufferCache mockCache;
    private int bufferSize = 128 * 1024;

    @Before
    public void setUp()
    {
        engine = new BlockingOutputEngine(bufferSize);
        mockCache = new MockBufferCache("test");
    }

    @Test
    public void testConstruction()
    {
        ///
        /// Tests the initial state
        ///
        assertEquals("STOPPED", engine.getPresentState());
        assertTrue(engine.isStopped());
        assertFalse(engine.isRunning());
        assertFalse(engine.isConnected());
        assertFalse(engine.isDestroyed());
        assertEquals(0, engine.getNumberOfChannels());
        assertEquals(0, engine.getDepth().length);
        assertEquals(0, engine.getRecordsSent());
        assertEquals(0, engine.getTotalRecordsSent());
        assertEquals(null, engine.getChannel());

        // starting with no channel should result in STOPPED
        engine.start();
        engine.startProcessing();
        assertEquals("STOPPED", engine.getPresentState());

        engine.destroyProcessor();
        assertEquals("DESTROYED", engine.getPresentState());
        assertTrue(engine.isDestroyed());
        assertFalse(engine.isRunning());
        assertFalse(engine.isConnected());
        assertFalse(engine.isStopped());
        assertEquals(0, engine.getNumberOfChannels());
        assertEquals(0, engine.getDepth().length);
        assertEquals(0, engine.getRecordsSent());
        assertEquals(0, engine.getTotalRecordsSent());
        assertEquals(null, engine.getChannel());

    }

    @Test
    public void testConnect()
            throws Exception
    {
        ///
        /// Test connect() and addChannel()
        ///

        QueuedOutputChannel channel =
            engine.connect(mockCache, new MockChannel(), 99);

        assertNotNull(channel);
        assertTrue(engine.isConnected());
        assertTrue(engine.isStopped());
        assertEquals(1, engine.getNumberOfChannels());
        assertEquals(0, engine.getDepth()[0]);

        assertSame(channel, engine.getChannel());

        //attempt a second connection
        try
        {
            engine.connect(mockCache, new MockChannel(), 33);
            fail("multiple connections allowed");
        }
        catch (Throwable th)
        {
            String expected = "Multiple connections not supported";
            assertEquals(expected, th.getMessage());
        }
    }


    /**
     * Test starting and stopping engine.
     */
    @Test
    public void testStartStop()
            throws Exception
    {

        //
        // Tests start/stop lifecycle
        //

        QueuedOutputChannel transmitEng;
        MockChannel sink = new MockChannel();

        engine.start();
        assertTrue(engine.isStopped());

        transmitEng = engine.addDataChannel(sink, mockCache, "SSChan");

        engine.startProcessing();
        assertTrue(engine.isRunning());

        transmitEng.receiveByteBuffer(mockCache.acquireBuffer(123));

        engine.forcedStopProcessing(); //generates a message
        assertTrue(engine.isStopped());

        assertEquals(123 + 4, sink.written); //4-byte stop message
        assertEquals(2, engine.getRecordsSent());
        assertEquals(2, engine.getTotalRecordsSent());
        assertFalse(sink.isOpen());


        // try it a second time
        sink = new MockChannel();

        transmitEng = engine.addDataChannel(sink, mockCache, "SSChan2");

        assertEquals(0, sink.written);
        assertEquals(0, engine.getRecordsSent());
        assertEquals(2, engine.getTotalRecordsSent());

        engine.startProcessing();
        assertTrue(engine.isRunning());

        transmitEng.receiveByteBuffer(mockCache.acquireBuffer(888));

        engine.forcedStopProcessing(); //generates a message
        assertTrue(engine.isStopped());
        assertEquals(888 + 4, sink.written); //4-byte stop message
        assertEquals(2, engine.getRecordsSent());
        assertEquals(4, engine.getTotalRecordsSent());
        assertFalse(sink.isOpen());


        // now try stopping via stop message
        sink = new MockChannel();
        transmitEng = engine.addDataChannel(sink, mockCache, "SSStop");

        engine.startProcessing();
        assertTrue(engine.isRunning());
        assertEquals(0, sink.written);
        assertEquals(0, engine.getRecordsSent());
        assertEquals(4, engine.getTotalRecordsSent());

        transmitEng.receiveByteBuffer(mockCache.acquireBuffer(17));

        engine.sendLastAndStop(); //generates a message
        assertTrue(engine.isStopped());

        assertEquals(17 + 4, sink.written); //4-byte stop message
        assertEquals(2, engine.getRecordsSent());
        assertEquals(6, engine.getTotalRecordsSent());
        assertFalse(sink.isOpen());

        // stop via sendLastAndStop() on channel
        sink = new MockChannel();

        transmitEng = engine.addDataChannel(sink, mockCache, "SSSndLst");

        assertEquals(0, sink.written);
        assertEquals(0, engine.getRecordsSent());
        assertEquals(6, engine.getTotalRecordsSent());

        engine.startProcessing();
        assertTrue(engine.isRunning());

        transmitEng.receiveByteBuffer(mockCache.acquireBuffer(888));

        transmitEng.sendLastAndStop();//generates a message

        assertEquals(888 + 4, sink.written); //4-byte stop message
        assertEquals(2, engine.getRecordsSent());
        assertEquals(8, engine.getTotalRecordsSent());
        assertFalse(sink.isOpen());

        assertFalse(engine.isRunning());
        assertTrue(engine.isStopped());


        // stop via sendLastAndStop() on channel engine
        sink = new MockChannel();
        transmitEng = engine.addDataChannel(sink, mockCache, "SSSndLst2");

        engine.startProcessing();
        assertTrue(engine.isRunning());
        assertEquals(0, sink.written);
        assertEquals(0, engine.getRecordsSent());
        assertEquals(8, engine.getTotalRecordsSent());

        transmitEng.receiveByteBuffer(mockCache.acquireBuffer(17));

        engine.sendLastAndStop(); //generates a message
        assertTrue(engine.isStopped());

        assertEquals(17 + 4, sink.written); //4-byte stop message
        assertEquals(2, engine.getRecordsSent());
        assertEquals(10, engine.getTotalRecordsSent());
        assertFalse(sink.isOpen());


        engine.destroyProcessor();
            assertTrue(engine.isDestroyed());

        try {
            engine.startProcessing();
            fail("Engine restart after destroyed succeeded");
        } catch (Throwable th) {
            // desired
            String expected = "Engine should be stopped, not DESTROYED";
            assertEquals(expected, th.getMessage());
        }
    }

    @Test
    public void testOutput()
            throws Exception
    {

        ///
        /// Test writing data through the engine
        ///

        // create a pipe for use in testing
        MockChannel sink = new MockChannel();

        engine.start();
        assertTrue(engine.isStopped());


        QueuedOutputChannel channel =
            engine.addDataChannel(sink, mockCache, "SSOut");

        engine.startProcessing();
        assertTrue(engine.isRunning());


        // fill the buffer
        int sent = 0;
        int msgCount = 0;
        while (sent < bufferSize)
        {
            int msgSize = randomSize(1, bufferSize - sent);
            ByteBuffer msg = mockCache.acquireBuffer(msgSize);
            channel.receiveByteBuffer(msg);
            msgCount++;
            sent+=msgSize;

            assertTrue(channel.isOutputQueued());
            assertEquals(msgCount, engine.getDepth()[0]);
            assertEquals(0, engine.getRecordsSent());
            assertEquals(0, engine.getTotalRecordsSent());
        }

        assertEquals(0, sink.written);

        // exceed the buffer
        channel.receiveByteBuffer(mockCache.acquireBuffer(1));

        assertTrue(channel.isOutputQueued());
        assertEquals(1, engine.getDepth()[0]);
        assertEquals(msgCount, engine.getRecordsSent());
        assertEquals(msgCount, engine.getTotalRecordsSent());
        assertEquals(sent, sink.written);

        //flush
        channel.flushOutQueue();
        assertFalse(channel.isOutputQueued());
        assertEquals(0, engine.getDepth()[0]);
        assertEquals(msgCount+1, engine.getRecordsSent());
        assertEquals(msgCount+1, engine.getTotalRecordsSent());
        assertEquals(sent+1, sink.written);

        // fill the buffer again
        int sent2 = 0;
        int msgCount2 = 0;
        while (sent < bufferSize)
        {
            int msgSize = randomSize(1, bufferSize - sent2);
            channel.receiveByteBuffer(mockCache.acquireBuffer(msgSize));
            msgCount2++;
            sent2+=msgSize;

            assertTrue(channel.isOutputQueued());
            assertEquals(msgCount2, engine.getDepth()[0]);
            assertEquals(0, engine.getRecordsSent());
            assertEquals(0, engine.getTotalRecordsSent());
        }


        // stop (should auto-flush)
        engine.sendLastAndStop();  //generates a message
        assertTrue(engine.isStopped());
        assertFalse(channel.isOutputQueued());
        assertEquals(0, engine.getDepth().length);
        assertEquals(msgCount + 1 + msgCount2 + 1, engine.getRecordsSent());
        assertEquals(msgCount + 1 + msgCount2 + 1, engine.getTotalRecordsSent());
        assertEquals(sent + 1 + sent2 + 4, sink.written); //4-byte stop message

        assertTrue("ByteBufferCache is not balanced", mockCache.isBalanced());
    }


    @Test
    public void testAutoFlushOutput()
            throws Exception
    {

        ///
        /// Test autoflush of channel
        ///
        final int AUTOFLUSH_PERIOD = 100;


        // create a pipe for use in testing
        MockChannel sink = new MockChannel();
        engine = new BlockingOutputEngine(bufferSize, true, AUTOFLUSH_PERIOD);

        engine.start();
        assertTrue(engine.isStopped());


        QueuedOutputChannel channel =
            engine.addDataChannel(sink, mockCache, "AFOut");

        engine.startProcessing();
        assertTrue(engine.isRunning());


        // fill the buffer
        int sent = 0;
        int msgCount = 0;
        while (sent < bufferSize)
        {
            int msgSize = randomSize(1, bufferSize - sent);
            ByteBuffer msg = mockCache.acquireBuffer(msgSize);
            channel.receiveByteBuffer(msg);
            msgCount++;
            sent+=msgSize;


        }

        // wait for an automatic flushing of messages
        try{ Thread.sleep(AUTOFLUSH_PERIOD*2);} catch (InterruptedException e){}
        assertFalse(channel.isOutputQueued());
        assertEquals(0, engine.getDepth()[0]);
        assertEquals(msgCount, engine.getRecordsSent());
        assertEquals(msgCount, engine.getTotalRecordsSent());
        assertEquals(bufferSize, sink.written);


        // write a single byte an wait for autoflush
        ByteBuffer msg = mockCache.acquireBuffer(1);
        channel.receiveByteBuffer(msg);
        msgCount++;
        try{ Thread.sleep(AUTOFLUSH_PERIOD*2);} catch (InterruptedException e){}
        assertFalse(channel.isOutputQueued());
        assertEquals(0, engine.getDepth()[0]);
        assertEquals(msgCount, engine.getRecordsSent());
        assertEquals(msgCount, engine.getTotalRecordsSent());
        assertEquals(bufferSize + 1, sink.written);

        // stop (should auto-flush)
        engine.sendLastAndStop();
        assertTrue(engine.isStopped());
        assertFalse(channel.isOutputQueued());
        assertEquals(0, engine.getDepth().length);
        assertEquals(msgCount + 1, engine.getRecordsSent());
        assertEquals(msgCount + 1, engine.getTotalRecordsSent());
        assertEquals(bufferSize + 1 + 4, sink.written); //4-byte stop message

        assertTrue("ByteBufferCache is not balanced", mockCache.isBalanced());
    }

    private static int randomSize(int min, int max)
    {
        return Math.max(min, ((int)(Math.random() * max)) );
    }

    @Test
    public void testDisconnect()
            throws Exception
    {

        engine.start();
        assertTrue(engine.isStopped());

        MockChannel sink = new MockChannel();


        QueuedOutputChannel transmitEng = engine.connect(mockCache, sink, 1);


        assertTrue(engine.isStopped());
        engine.startProcessing();
        assertTrue(engine.isRunning());

        engine.disconnect();
        assertTrue(engine.isStopped());

        assertFalse(sink.isOpen());


        assertTrue("ByteBufferCache is not balanced", mockCache.isBalanced());
    }


    private static class MockChannel implements WritableByteChannel
    {
        boolean isOpen = true;
        long written;

        @Override
        public int write(final ByteBuffer src) throws IOException
        {
            if(isOpen)
            {
                int remaining = src.remaining();
                written+=remaining;
                src.position(src.limit());
                return remaining;
            }
            else
            {
                throw new IOException("Write when closed.");
            }
        }

        @Override
        public boolean isOpen()
        {
            return isOpen;
        }

        @Override
        public void close() throws IOException
        {
            isOpen = false;
        }
    }


}
