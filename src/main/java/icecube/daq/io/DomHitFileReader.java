package icecube.daq.io;

import icecube.daq.payload.IDomHit;
import icecube.daq.payload.PayloadFormatException;
import icecube.daq.payload.PayloadRegistry;
import icecube.daq.payload.impl.DeltaCompressedHitData;
import icecube.daq.payload.impl.EngineeringHitData;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class DomHitFactory
{
    private static Log LOG = LogFactory.getLog(DomHitFactory.class);

    IDomHit createPayload(int offset, ByteBuffer buf)
        throws IOException, PayloadFormatException
    {
        if (offset != 0) {
            throw new Error("Offset should always be zero");
        }

        if (buf.limit() < offset + 4) {
            throw new Error("Expected buffer with at least " + (offset + 4) +
                            " bytes, not " + buf.limit() + " (offset=" +
                            offset + ")");
        }

        final int len = buf.getInt(offset + 0);
        if (buf.limit() < offset + len) {
            throw new Error("Payload at offset " + offset + " requires " +
                            len + " bytes, but buffer limit is " + buf.limit());
        }

        final int type = buf.getInt(offset + 4);
        switch (type) {
        case 2:
            throw new Error("EngineeringHit reader unimplemented");
/*
            EngineeringHitData engHit = new EngineeringHitData(buf, offset);
            return engHit;
*/
        case 3:
        case PayloadRegistry.PAYLOAD_ID_DELTA_HIT:
            throw new Error("DeltaCompressedHit reader unimplemented");
/*
            // XXX rewrite payload type to match real payload type
            final int payType =
                PayloadRegistry.PAYLOAD_ID_COMPRESSED_HIT_DATA;
            buf.putInt(offset + 4, payType);
            DeltaCompressedHitData deltaHit =
                new DeltaCompressedHitData(buf, offset);
            return deltaHit;
*/
        default:
            break;
        }

        LOG.error("Ignoring unknown hit type " + type + " in " + len +
                  "-byte payload");
        return null;
    }
}

/**
 * Read dom hits from a file.
 */
public class DomHitFileReader
    implements Iterable<IDomHit>, Iterator<IDomHit>
{
    /** Input channel */
    private ReadableByteChannel chan;
    /** Factory used to build payloads */
    private DomHitFactory factory;
    /** ByteBuffer used to read the hit length */
    private ByteBuffer lenBuf;

    /** <tt>true</tt> if we've checked for another hit */
    private boolean gotNext;
    /** Next available hit */
    private IDomHit nextHit;

    /**
     * Open the named file.
     *
     * @param name file name
     *
     * @throws IOException if the file cannot be opened
     */
    public DomHitFileReader(String name)
        throws IOException
    {
        this(new File(name));
    }

    /**
     * Open the file.
     *
     * @param file hit file
     *
     * @throws IOException if the file cannot be opened
     */
    public DomHitFileReader(File file)
        throws IOException
    {
        this(new FileInputStream(file));
    }

    /**
     * Use the specified stream to read hits.
     *
     * @param stream hit file stream
     */
    public DomHitFileReader(FileInputStream stream)
    {
        this(stream.getChannel());
    }

    /**
     * Use the specified channel to read hits.
     *
     * @param chan hit file channel
     */
    public DomHitFileReader(ReadableByteChannel chan)
    {
        this.chan = chan;

        factory = new DomHitFactory();
    }

    /**
     * Close the file.
     *
     * @throws IOException if there was a problem closing the file
     */
    public void close()
        throws IOException
    {
        if (chan != null) {
            try {
                chan.close();
            } finally {
                chan = null;
            }
        }
    }

    /**
     * Read the next hit from the file.
     *
     * @throws IOException if there is a problem with the next hit
     * @throws IOException if the next hit cannot be read
     */
    private void getNextHit()
        throws IOException, PayloadFormatException
    {
        if (lenBuf == null) {
            lenBuf = ByteBuffer.allocate(4);
        }

        gotNext = true;
        nextHit = null;

        lenBuf.rewind();
        int numBytes = chan.read(lenBuf);
        if (numBytes >= 4) {
            int len = lenBuf.getInt(0);
            if (len < 4) {
                throw new PayloadFormatException("Bad length " + len);
            }

            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putInt(len);
            chan.read(buf);

            nextHit = factory.createPayload(0, buf);
        }
    }

    /**
     * Is another hit available?
     *
     * @return <tt>true</tt> if there is another hit
     */
    public boolean hasNext()
    {
        if (!gotNext) {
            try {
                getNextHit();
            } catch (PayloadFormatException pfe) {
                nextHit = null;
            } catch (IOException ioe) {
                nextHit = null;
            }
        }

        return nextHit != null;
    }

    /**
     * This object is an iterator for itself.
     *
     * @return this object
     */
    public Iterator iterator()
    {
        return this;
    }

    /**
     * Get the next available hit.
     */
    public IDomHit next()
    {
        try {
            return nextHit();
        } catch (PayloadFormatException ioe) {
            return null;
        } catch (IOException ioe) {
            return null;
        }
    }

    /**
     * Get the next available hit.
     *
     * @return next hit (or <tt>null</tt>)
     *
     * @throws IOException if there is a problem with the next hit
     * @throws IOException if the next hit cannot be read
     */
    public IDomHit nextHit()
        throws IOException, PayloadFormatException
    {
        if (!gotNext) {
            getNextHit();
        }

        gotNext = false;

        return nextHit;
    }

    /**
     * Unimplemented.
     */
    public void remove()
    {
        throw new Error("Unimplemented");
    }
}
