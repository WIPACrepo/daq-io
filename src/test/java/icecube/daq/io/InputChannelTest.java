package icecube.daq.io;

import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class InputChannelTest
    extends LoggingCase
{
    class MockParent
        implements IOChannelParent
    {
        public void channelError(IOChannel chan, ByteBuffer buf, Exception ex)
        {
            throw new Error("Unimplemented");
        }

        public void channelStopped(IOChannel chan)
        {
            throw new Error("Unimplemented");
        }
    }

    class MockChannel
        extends InputChannel
    {
        public MockChannel(IOChannelParent parent, SelectableChannel channel,
                         IByteBufferCache bufMgr, int bufSize)
            throws IOException
        {
            super(parent, channel, bufMgr, bufSize);
        }

        public void pushPayload(ByteBuffer payBuf)
            throws IOException
        {
            throw new Error("Unimplemented");
        }

        public void registerComponentObserver(DAQComponentObserver compObserver,
                                              String notificationID)
        {
            throw new Error("Unimplemented");
        }
    }

    /**
     * Constructs an instance of this test.
     *
     * @param name the name of the test.
     */
    public InputChannelTest(String name)
    {
        super(name);
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(InputChannelTest.class);
    }

    public void testAllocLimits()
        throws IOException
    {
        MockParent parent = new MockParent();

        Pipe pipe = Pipe.open();

        final long[] vals = new long[] {
            Long.MIN_VALUE, -1L, Long.MIN_VALUE / 2,
            0L, 1L, 256L,
            Long.MAX_VALUE, Long.MAX_VALUE - 1L, Long.MAX_VALUE / 2
        };

        for (int i = 0; i < vals.length; i++) {
            IByteBufferCache bufMgr;
            if (vals[i] == Long.MIN_VALUE) {
                bufMgr = new MockBufferCache("AllocLim");
            } else {
                bufMgr = new MockBufferCache("AllocLim", vals[i]);
            }

            InputChannel chan =
                new MockChannel(parent, pipe.source(), bufMgr, 256);

            final long maxAlloc;
            if (vals[i] <= 0) {
                maxAlloc = InputChannel.DEFAULT_MAX_BYTES_ALLOCATION_LIMIT;
            } else {
                maxAlloc = vals[i];
            }

            final long expStop =
                ((maxAlloc / 100L) * InputChannel.PERCENT_STOP_ALLOCATION) +
                (((maxAlloc % 100L) * InputChannel.PERCENT_STOP_ALLOCATION) /
                 100L);
            final long expRestart =
                ((maxAlloc / 100L) * InputChannel.PERCENT_RESTART_ALLOCATION) +
                (((maxAlloc % 100L) * InputChannel.PERCENT_RESTART_ALLOCATION) /
                 100L);

            assertEquals("Bad stop allocation limit",
                         expStop, chan.getLimitToStopAllocation());
            assertEquals("Bad restart allocation limit",
                         expRestart, chan.getLimitToRestartAllocation());
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
