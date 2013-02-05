package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.impl.PayloadFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Read payloads from a file.
 */
public class PayloadFileReader
    implements DAQFileReader
{
    private static final Log LOG = LogFactory.getLog(PayloadFileReader.class);

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
     * Open the file.
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
     * @param cache byte buffer cache
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
    public Iterator iterator()
    {
        return this;
    }

    /**
     * Get the next available payload.
     */
    public Object next()
    {
        try {
            return nextPayload();
        } catch (PayloadException pe) {
            LOG.error("Cannot return next payload", pe);
            return null;
        }
    }

    /**
     * Get the next available payload.
     *
     * @return next payload (or <tt>null</tt>)
     *
     * @throws PayloadException if there is a problem with the next payload
     */
    public IPayload nextPayload()
        throws PayloadException
    {
        if (rdr == null) {
            throw new PayloadException("Reader for has been closed");
        }

        ByteBuffer buf = rdr.nextBuffer();
        if (buf == null) {
            return null;
        }

        return factory.getPayload(buf, 0);
    }

    /**
     * Unimplemented.
     */
    public void remove()
    {
        throw new Error("Unimplemented");
    }
}
