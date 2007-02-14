package icecube.daq.io.test;

import icecube.daq.splicer.Spliceable;

import java.nio.ByteBuffer;

public class MockSpliceable
    implements Spliceable
{
    private static int nextId = 1;

    private int id;
    private ByteBuffer bBuf;

    public MockSpliceable(ByteBuffer bBuf)
    {
        id = nextId++;
        this.bBuf = bBuf;
    }

    public int compareTo(Object obj)
    {
        if (obj == null) {
            return -1;
        }

        if (!(obj instanceof MockSpliceable)) {
            return getClass().getName().compareTo(obj.getClass().getName());
        }

        return id - ((MockSpliceable) obj).id;
    }
}
