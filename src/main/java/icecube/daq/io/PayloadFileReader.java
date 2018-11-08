package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.impl.PayloadFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.log4j.Logger;

/**
 * Read a payload file and return the data in IPayload objects.
 */
public class PayloadFileReader
    implements Iterator<IPayload>, Iterable<IPayload>
{
    private static final Logger LOG = Logger.getLogger(PayloadFileReader.class);

    /** Payload byte buffer reader */
    private PayloadByteReader rdr;
    /** Factory used to build payloads */
    private PayloadFactory factory;

    /**
     * Open the named file.
     *
     * @param name file name
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadFileReader(String name)
        throws IOException
    {
        this(new File(name), null);
    }

    /**
     * Open the named file.
     *
     * @param name file name
     * @param cache byte buffer cache
     *
     * @throws IOException if the file cannot be opened
     */
    private PayloadFileReader(String name, IByteBufferCache cache)
        throws IOException
    {
        this(new File(name), cache);
    }

    /**
     * Open the file.
     *
     * @param payFile payload file
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadFileReader(File payFile)
        throws IOException
    {
        this(payFile, null);
    }

    /**
     * Open the file.
     *
     * @param payFile payload file
     * @param cache byte buffer cache
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadFileReader(File payFile, IByteBufferCache cache)
        throws IOException
    {
        rdr = new PayloadByteReader(payFile);
        factory = new PayloadFactory(cache);
    }

    /**
     * Close the file.
     *
     * @throws IOException if there was a problem closing the file
     */
    public void close()
        throws IOException
    {
        if (rdr != null) {
            try {
                rdr.close();
            } finally {
                rdr = null;
            }
        }
    }

    /**
     * Is another payload available?
     *
     * @return <tt>true</tt> if there is another payload
     *
     * @throws Error if this reader has been closed
     */
    @Override
    public boolean hasNext()
    {
        if (rdr == null) {
            throw new Error("Reader has been closed");
        }

        return rdr.hasNext();
    }

    /**
     * This object is an iterator for itself.
     *
     * @return this object
     */
    @Override
    public Iterator<IPayload> iterator()
    {
        return this;
    }

    /**
     * Get the next available payload.
     */
    @Override
    public IPayload next()
    {
        if (rdr != null) {
            ByteBuffer buf;
            try {
                buf = rdr.nextBuffer();
                if (buf != null) {
                    return factory.getPayload(buf, 0);
                }
            } catch (PayloadException pe) {
                LOG.error("Cannot return next payload", pe);
            }
        }

        return null;
    }

    /**
     * Unimplemented.
     */
    @Override
    public void remove()
    {
        throw new Error("Unimplemented");
    }
}
