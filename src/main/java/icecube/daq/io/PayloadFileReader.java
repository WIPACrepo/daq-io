package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.impl.PayloadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
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

    /** Input channel */
    private ReadableByteChannel chan;
    /** Factory used to build payloads */
    private PayloadFactory factory;
    /** ByteBuffer used to read the payload length */
    private ByteBuffer lenBuf;

    /** <tt>true</tt> if we've checked for another payload */
    private boolean gotNext;
    /** Next available payload */
    private IPayload nextPayload;

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
     * @param file payload file
     * @param cache byte buffer cache
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadFileReader(File file)
        throws IOException
    {
        this(file, null);
    }

    /**
     * Open the file.
     *
     * @param file payload file
     * @param cache byte buffer cache
     *
     * @throws IOException if the file cannot be opened
     */
    private PayloadFileReader(File file, IByteBufferCache cache)
        throws IOException
    {
        this(new FileInputStream(file), cache);
    }

    /**
     * Use the specified stream to read payloads.
     *
     * @param stream payload file stream
     * @param cache byte buffer cache
     */
    private PayloadFileReader(FileInputStream stream, IByteBufferCache cache)
    {
        this(stream.getChannel(), cache);
    }

    /**
     * Use the specified channel to read payloads.
     *
     * @param chan payload file channel
     * @param cache byte buffer cache
     */
    private PayloadFileReader(ReadableByteChannel chan, IByteBufferCache cache)
    {
        this.chan = chan;

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
        if (chan != null) {
            try {
                chan.close();
            } finally {
                chan = null;
            }
        }
    }

    /**
     * Read the next payload from the file.
     *
     * @throws IOException if there is a problem with the next payload
     * @throws IOException if the next payload cannot be read
     */
    private void getNextPayload()
        throws PayloadException
    {
        if (lenBuf == null) {
            lenBuf = ByteBuffer.allocate(4);
        }

        gotNext = true;
        nextPayload = null;

        lenBuf.rewind();

        int numBytes;
        try {
            numBytes = chan.read(lenBuf);
        } catch (IOException ioe) {
            throw new PayloadException("Cannot read length of next payload",
                                       ioe);
        }

        if (numBytes >= 4) {
            int len = lenBuf.getInt(0);
            if (len < 4) {
                throw new PayloadException("Bad length " + len);
            }

            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putInt(len);
            try {
                chan.read(buf);
            } catch (IOException ioe) {
                throw new PayloadException("Cannot read next payload (" + len +
                                           " bytes)", ioe);
            }

            nextPayload = factory.getPayload(buf, 0);
        }
    }

    /**
     * Is another payload available?
     *
     * @return <tt>true</tt> if there is another payload
     */
    public boolean hasNext()
    {
        if (!gotNext) {
            try {
                getNextPayload();
            } catch (PayloadException pe) {
                LOG.error("Cannot get next payload", pe);
                nextPayload = null;
            }
        }

        return nextPayload != null;
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
        if (!gotNext) {
            getNextPayload();
        }

        gotNext = false;

        return nextPayload;
    }

    /**
     * Unimplemented.
     */
    public void remove()
    {
        throw new Error("Unimplemented");
    }
}
