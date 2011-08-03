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

public class DispatchExceptionTest
    extends LoggingCase
{
     public DispatchExceptionTest(String name)
    {
        super(name);
    }
     public static Test suite()
    {
        return new TestSuite(DispatchExceptionTest.class);
    }
    public void testConstructor()
    {
	DispatchException de;
	de = new DispatchException();

	de = new DispatchException("Exception", new Error("Cause Unknown"));
	de = new DispatchException(new Error("Cause Unknown"));

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
