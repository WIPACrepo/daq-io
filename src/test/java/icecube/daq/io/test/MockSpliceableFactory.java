package icecube.daq.io.test;

import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SpliceableFactory;

import java.nio.ByteBuffer;

import java.util.List;

public class MockSpliceableFactory
    implements SpliceableFactory
{
    public MockSpliceableFactory()
    {
    }

    public void backingBufferShift(List x0, int i1, int i2)
    {
try{throw new Error("StackTrace");}catch(Error e){e.printStackTrace();}
        throw new Error("Unimplemented");
    }

    public Spliceable createCurrentPlaceSpliceable()
    {
try{throw new Error("StackTrace");}catch(Error e){e.printStackTrace();}
        throw new Error("Unimplemented");
    }

    public Spliceable createSpliceable(ByteBuffer bBuf)
    {
        return new MockSpliceable(bBuf);
    }

    public void invalidateSpliceables(List x0)
    {
try{throw new Error("StackTrace");}catch(Error e){e.printStackTrace();}
        throw new Error("Unimplemented");
    }

    public boolean skipSpliceable(ByteBuffer x0)
    {
try{throw new Error("StackTrace");}catch(Error e){e.printStackTrace();}
        throw new Error("Unimplemented");
    }
}
