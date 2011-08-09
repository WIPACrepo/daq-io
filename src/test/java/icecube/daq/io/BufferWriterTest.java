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

public class BufferWriterTest
    extends LoggingCase
{
     public BufferWriterTest(String name)
    {
        super(name);
    }
     public static Test suite()
    {
        return new TestSuite(BufferWriterTest.class);
    }

    public void testGoodFile() throws IOException
    {
	BufferWriter bw;
	bw = new BufferWriter("subdir");
    }
	
    public void testAppendFile() throws IOException
    {
	BufferWriter bw;
	bw = new BufferWriter("subdir", true);
    }

    public void testOpenNext() throws IOException
    {
	String baseName = "subdir";
	BufferWriter bw;
	bw = new BufferWriter(baseName);
	assertNotNull("Buffer writer object should be returned",
	    bw.openNext(baseName));
    }

    public void testWrites() throws IOException
    {
	ByteBuffer buf = ByteBuffer.allocate(12);
	BufferWriter bw;
	bw = new BufferWriter("subdir", true);

	assertEquals("Number of bytes written", 12, bw.write(buf));
	assertEquals("Number of bytes written", 12, bw.writeAndRestore(buf));
	assertEquals("Number of bytes written", 12,
	    bw.writeAndRestore(buf, 0));

	try {
	    bw.writeAndRestore(null, 0);
	}
	catch(Exception e) {
	    if(!(e.getMessage().equals("Cannot write null buffer"))) {
		throw new Error ("ByteBuffer cannot be null");
	    }
	}
    }

    public void testClose() throws IOException
    {
	BufferWriter bw;
	bw = new BufferWriter("subdir",true);
	bw.close();

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
