package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadDestination;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.IWriteablePayload;
import icecube.daq.payload.Poolable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.zip.DataFormatException;


import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class DepthQueueTest
    extends LoggingCase
{
     public DepthQueueTest(String name)
    {
        super(name);
    }
     public static Test suite()
    {
        return new TestSuite(DepthQueueTest.class);
    }

    public void testgetDepth()
    {
	DepthQueue dq = new DepthQueue();
	assertEquals("Current depth value returned", 0 , dq.getDepth());
    }
    public void testisEmpty()
    {
	DepthQueue dq = new DepthQueue();
	assertEquals("if queue is empty", true , dq.isEmpty());
    }
    public void testput() throws Exception
    {
	ByteBuffer buf = ByteBuffer.allocate(12);
	DepthQueue dq = new DepthQueue();
	dq.put(buf);
	dq.take();
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
