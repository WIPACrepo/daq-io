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
import java.util.Set;
import java.util.HashSet;
import java.lang.Object;

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
	//SelectionKey s;
	//return s;
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

class mockSelector
    extends Selector
{
    public mockSelector()
    {
    }
    public Selector wakeup()
    {
	throw new Error("Unimplemented");
    }
    public int select()
    {
	return 1;
    }
    public boolean isOpen()
    {
	return false;
    }
    public int selectNow()
    {
	return 1;
    }
    public int select(long l)
    {
	return 1;
    }
    public Set selectedKeys()
    {
	Set set1 = new HashSet();
	return set1;
    }
    public Set keys()
    {
	Set set1 = new HashSet();
	return set1;
    }
    public SelectorProvider provider()
    {
	throw new Error("Unimplemented");
    }
    public void close()
    {
    }
}

class MyCache
    implements IByteBufferCache
{
    public MyCache()
    {
    }

    public ByteBuffer acquireBuffer(int len)
    {
        return ByteBuffer.allocate(len);
    }

    public void destinationClosed()
    {
        throw new Error("Unimplemented");
    }

    public void flush()
    {
        throw new Error("Unimplemented");
    }

    public int getCurrentAquiredBuffers()
    {
        throw new Error("Unimplemented");
    }

    public long getCurrentAquiredBytes()
    {
        throw new Error("Unimplemented");
    }

    public boolean getIsCacheBounded()
    {
        throw new Error("Unimplemented");
    }

    public long getMaxAquiredBytes()
    {
        throw new Error("Unimplemented");
    }

    public String getName()
    {
        throw new Error("Unimplemented");
    }

    public int getTotalBuffersAcquired()
    {
        throw new Error("Unimplemented");
    }

    public int getTotalBuffersCreated()
    {
        throw new Error("Unimplemented");
    }

    public int getTotalBuffersReturned()
    {
        throw new Error("Unimplemented");
    }

    public long getTotalBytesInCache()
    {
        throw new Error("Unimplemented");
    }

    public boolean isBalanced()
    {
        throw new Error("Unimplemented");
    }

    public void receiveByteBuffer(ByteBuffer x0)
    {
        throw new Error("Unimplemented");
    }

    public void returnBuffer(ByteBuffer x0)
    {
        // do nothing
    }

    public void returnBuffer(int x0)
    {
        // do nothing
    }
}


public class PayloadTransmitChannelTest
    extends LoggingCase
{
     public PayloadTransmitChannelTest(String name)
    {
        super(name);
    }
     public static Test suite()
    {
        return new TestSuite(PayloadTransmitChannelTest.class);
    }


     public void testMethods() throws Exception
    {
	final String myId = "payload";
	
	ByteBuffer buf = ByteBuffer.allocate(10);

	PayloadTransmitChannel ptc;
	WritableByteChannel channel;
        Selector sel;
        IByteBufferCache bufMgr;
	channel = new mockWritableByteChannel();
	sel = new mockSelector();
	bufMgr = new MyCache();
	ptc = new PayloadTransmitChannel(myId, channel, sel, bufMgr);

	ptc.stopEngine();
	ptc.sendLastAndStop();
	//ptc.startEngine();
	//ptc.flushOutQueue();
	ptc.injectError();
	ptc.processTimer();
	ptc.close();
	//ptc.receiveByteBuffer( buf);
	//ptc.destinationClosed();

/*
	sd.allPayloadDestinationsClosed();
	assertNull("reference to buffer manager", sd.getBufferManager());
	assertNotNull("number of messages sent", sd.getMessagesSent());
	assertNotNull("PayloadDestinationCollection", sd.getPayloadDestinationCollection());
	sd.payloadDestinationClosed(sourceId);
	try {
	sd.sendPayload( sourceId, buf);
	} catch (Exception e) {
	if(!e.getMessage().equals("SourceID unknownComponent#1#1not registered")) {
	throw new Error("SourceID unknownComponent#1#1not registered");
	}
	}
	assertNull("output channel associated with the specified source ID", sd.lookUpEngineBySourceID(sourceId)); */
	
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
