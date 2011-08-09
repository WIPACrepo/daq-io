package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadDestination;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.IWriteablePayload;
import icecube.daq.payload.IDOMID;
import icecube.daq.payload.IEventHitRecord;
import icecube.daq.payload.IEventPayload;
import icecube.daq.payload.IHitData;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IReadoutDataPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.Poolable;
import icecube.daq.util.DOMRegistry;
import icecube.daq.payload.impl.DOMID;
import icecube.daq.payload.impl.EventPayload_v5;
import icecube.daq.payload.impl.EventPayload_v6;
import icecube.daq.payload.impl.PayloadFactory;
import icecube.daq.payload.impl.SourceID;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.zip.DataFormatException;


import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

class mockWritableByteChannel
    extends SelectableChannel
    implements WritableByteChannel
    
{
    public mockWritableByteChannel()
    {
    }
    public int write(ByteBuffer buf)
    {
	return 1;
    }
    public Object blockingLock()
    {
	throw new Error("Unimplemented");
    }
    public boolean isBlocking()
    {
	return false;
    }
    public SelectableChannel configureBlocking(boolean bool)
    {
	throw new Error("Unimplemented");
    }
    public SelectionKey register(Selector s,int i,Object o)
    {
	throw new Error("Unimplemented");
    }
    public SelectionKey keyFor(Selector s)
    {
	throw new Error("Unimplemented");
    }
    public boolean isRegistered()
    {
	return false;
    }
    public int validOps()
    {
	return 1;
    }
    public SelectorProvider provider()
    {
	throw new Error("Unimplemented");
    }
    public void implCloseChannel()
    {
    }
}


public class SimpleDestinationOutputEngineTest
    extends LoggingCase
{
     public SimpleDestinationOutputEngineTest(String name)
    {
        super(name);
    }
     public static Test suite()
    {
        return new TestSuite(SimpleDestinationOutputEngineTest.class);
    }


     public void testMethods() throws Exception
    {
	final int srcId = 1;
	WritableByteChannel channel;
	
	ISourceID sourceId;
	sourceId = new SourceID(srcId);
	channel = new mockWritableByteChannel();
	ByteBuffer buf = ByteBuffer.allocate(10);
	SimpleDestinationOutputEngine sd;
	sd = new SimpleDestinationOutputEngine("electric", srcId, "function");
	sd.allPayloadDestinationsClosed();

	assertNull("reference to buffer manager", sd.getBufferManager());
	assertNotNull("number of messages sent", sd.getMessagesSent());
	assertNotNull("PayloadDestinationCollection", 
	    sd.getPayloadDestinationCollection());

	sd.payloadDestinationClosed(sourceId);

	try {
	    sd.sendPayload( sourceId, buf);
	} catch (Exception e) {
	    if(!e.getMessage().
		equals("SourceID unknownComponent#1#1not registered")) {
	            throw new Error("SrcID unknownComponent#11not registered");
	    }
	}

	assertNull("output channel associated with the specified source ID",
            sd.lookUpEngineBySourceID(sourceId));
	
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
