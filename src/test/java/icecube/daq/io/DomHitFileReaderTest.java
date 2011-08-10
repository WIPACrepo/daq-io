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
import icecube.daq.payload.IReadoutDataPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.Poolable;
import icecube.daq.util.DOMRegistry;
import icecube.daq.payload.impl.DOMID;
import icecube.daq.payload.impl.EventPayload_v5;
import icecube.daq.payload.impl.EventPayload_v6;
import icecube.daq.payload.impl.PayloadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.zip.DataFormatException;
import java.util.Iterator;


import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class DomHitFileReaderTest
    extends LoggingCase
{
    public DomHitFileReaderTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return new TestSuite(DomHitFileReaderTest.class);
    }

    public void testConstructor() throws Exception
    {
	File file = new File("subdir");
	FileInputStream fis = new FileInputStream(file);
	DomHitFileReader dh;
	dh = new DomHitFileReader("subdir");
	dh = new DomHitFileReader(file);
    }

    public void testClose() throws Exception
    {
	DomHitFileReader dh;
	dh = new DomHitFileReader("subdir");
	dh.close();
    }

    public void testhasNext() throws Exception
    {
	DomHitFileReader dh;
	dh = new DomHitFileReader("subdir");
	assertFalse("if there is another hit", dh.hasNext());
    }

    public void testIterator() throws Exception
    {
	DomHitFileReader dh;
	dh = new DomHitFileReader("subdir");
	Iterator itr = dh.iterator();
	assertNotNull("Iterator object returned", itr);
    }
    
    public void testNext() throws Exception
    {
	DomHitFileReader dh;
	dh = new DomHitFileReader("subdir");
	assertNull("Next hit returned", dh.next());
    }

    public void testNextHit() throws Exception
    {
	DomHitFileReader dh;
	dh = new DomHitFileReader("subdir");
    }

    public void testRemove() throws Exception
    {
	DomHitFileReader dh;
	dh = new DomHitFileReader("subdir");
	try {
	    dh.remove();
	}catch (Error e) {
	    if(!e.getMessage().equals("Unimplemented")) {
		throw new Error("Unimplemented");
	    }
	}
    }

    /*public void testDOMHitFactory() throws Exception
    {
	final int offset = 0;
	ByteBuffer buf = ByteBuffer.allocate(12);
	DomHitFactory dhf = new DomHitFactory();
	assertNull("DomHit object returned", dhf.createPayload( offset, buf));
    }*/
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
