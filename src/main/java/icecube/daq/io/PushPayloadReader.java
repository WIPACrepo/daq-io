package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.SelectableChannel;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class PushInputChannel
    extends InputChannel
{
    private PushPayloadReader reader;

    PushInputChannel(InputChannelParent parent, SelectableChannel channel,
                     IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        super(parent, channel, bufMgr, bufSize);

        reader = (PushPayloadReader) parent;
    }

    void notifyOnStop()
    {
        reader.channelStopped(this);
        super.notifyOnStop();
    }

    public void pushPayload(ByteBuffer payBuf)
        throws IOException
    {
        reader.pushBuffer(payBuf);
    }
}

public abstract class PushPayloadReader
    extends PayloadReader
{
    private static final Log LOG = LogFactory.getLog(PushPayloadReader.class);

    private ArrayList<InputChannel> chanList = new ArrayList<InputChannel>();

    // default maximum size of strand queue
    public PushPayloadReader(String name)
        throws IOException
    {
        super(name);
    }

    public InputChannel createChannel(SelectableChannel channel,
                                      IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        InputChannel chan =
            new PushInputChannel(this, channel, bufMgr, bufSize);
        chanList.add(chan);
        return chan;
    }

    void channelStopped(InputChannel chan)
    {
        chanList.remove(chan);
        if (chanList.size() == 0) {
            sendStop();
        }
    }

    public abstract void pushBuffer(ByteBuffer bb)
        throws IOException;

    public abstract void sendStop();
}
