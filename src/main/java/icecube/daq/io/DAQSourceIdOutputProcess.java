package icecube.daq.io;

import icecube.daq.payload.ISourceID;

import java.nio.channels.WritableByteChannel;

public interface DAQSourceIdOutputProcess
    extends DAQComponentOutputProcess
{
    OutputChannel addDataChannel(WritableByteChannel channel, ISourceID srcId);
}
