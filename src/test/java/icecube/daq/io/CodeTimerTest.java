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

public class CodeTimerTest
    extends LoggingCase
{
    public CodeTimerTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return new TestSuite(CodeTimerTest.class);
    }

    public void testConstructor()
    {
	final int numTimes = 10;
	CodeTimer ct;
	ct = new CodeTimer(numTimes);
    }

    public void testbadNum()
    {
	final int numTimes = 10;
	final int num = -1;
	final int num1 = 11;
	final long time = 5L;
	CodeTimer ct;
	ct = new CodeTimer(numTimes);

	try {
	    ct.addTime(num, time);
	}
	catch(Error e) {
	    if(!(e.getMessage().equals("Illegal timer #-1"))) {
		throw new Error ("Illegal num value");
	    }
	}

	try {
	    ct.addTime(num1, time);
	}
	catch(Error e) {
	    if(!(e.getMessage().equals("Illegal timer #11"))) {
		throw new Error ("Illegal num value");
	    }
	}
	
    }

    public void testaddTime()
    {
	final int numTimes = 10;
	final int num = 1;
	final long time = 5L;
	CodeTimer ct;
	ct = new CodeTimer(numTimes);

	assertEquals("Time value retuned", 5L, ct.addTime(num, time));
	
    }

    public void teststartStop()
    {
	final int numTimes = 10;

	CodeTimer ct;
	ct = new CodeTimer(numTimes);

	try {
	    ct.stop(1);
	}
	catch(Error e) {
	    if(!(e.getMessage().equals("No timer running"))) {
		throw new Error ("Start timer before stop");
	    }
	}

	ct.start();
	try {
	    ct.stop(-1);
	}
	catch(Error e) {
	    if(!(e.getMessage().equals("Illegal timer #-1"))) {
		throw new Error ("Illegal num value");
	    }
	}

	try {
	    ct.stop(11);
	}
	catch(Error e) {
	    if(!(e.getMessage().equals("Illegal timer #11"))) {
		throw new Error ("Illegal num value");
	    }
	}

	ct.stop(1);
    }

    public void testgetStats()
    {
	final int numTimes = 10;
	final String title = "CodeTimer";
	final String[] title1 = new String[10];
	final String[] title2 = new String[11];
	final long time = 5L;
	final long num = 10L;

	CodeTimer ct;
	ct = new CodeTimer(numTimes);

	try {
	    ct.getStats(title,-1);
	}
	catch(Error e) {
	    if(!(e.getMessage().equals("Illegal timer #-1"))) {
		throw new Error ("Illegal num value");
	    }
	}
	ct.addTime(0, time);
	ct.addTime(1, time);

	assertNotNull("Message returned", ct.getStats(title,1));
	assertNotNull("Message returned", ct.getStats(title, time, num, 1));
	assertNotNull("Message returned", ct.getStats(title1));

	try {
	    assertNotNull("Message returned", ct.getStats(title2));
	}
	catch(Error e) {
	    if(!(e.getMessage().equals("Expected 10 titles, got 11"))) {
		throw new Error ("Change title length");
	    }
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
