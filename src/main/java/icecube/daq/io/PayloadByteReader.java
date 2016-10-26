package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.PayloadException;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Read payload bytes from a file.
 */
public class PayloadByteReader
    implements Iterator<ByteBuffer>, Iterable<ByteBuffer>
{
    private static final Log LOG = LogFactory.getLog(PayloadByteReader.class);

    /** maximum payload length (to avoid garbage inputs) */
    private static final int MAX_PAYLOAD_LEN = 10000000;

    /** Input file */
    private File file;
    /** Input stream */
    private DataInputStream stream;
    /** Byte buffer cache */
    private IByteBufferCache cache;

    /** Next payload number (used for error reporting) */
    private int nextNum;
    /** <tt>true</tt> if we've checked for another payload */
    private boolean gotNext;
    /** Next available payload buffer */
    private ByteBuffer nextBuffer;

    /**
     * Open the named file.
     *
     * @param name file name
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadByteReader(String name)
        throws IOException
    {
        this(new File(name));
    }

    /**
     * Open the file.
     *
     * @param file payload file
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadByteReader(File file)
        throws IOException
    {
        this(file, null);
    }

    /**
     * Open the file.
     *
     * @param file payload file
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadByteReader(File file, IByteBufferCache cache)
        throws IOException
    {
        this(file, null, cache);
    }

    /**
     * Open the file.
     *
     * @param file payload file
     *
     * @throws IOException if the file cannot be opened
     */
/*
    public PayloadByteReader(File file, InputStream stream)
        throws IOException
    {
        this(file, stream, null);
    }
*/

    /**
     * Open the file.
     *
     * @param file payload file
     *
     * @throws IOException if the file cannot be opened
     */
    private PayloadByteReader(File file, InputStream stream,
                              IByteBufferCache cache)
        throws IOException
    {
        this.file = file;
        this.cache = cache;

        FileInputStream fin = new FileInputStream(file);

        stream = new BufferedInputStream(fin);
        if (file.getName().endsWith(".gz")) {
            stream = new GZIPInputStream(stream);
        }

        this.stream = new DataInputStream(stream);
    }

    /**
     * Close the file.
     *
     * @throws IOException if there was a problem closing the file
     */
    public void close()
        throws IOException
    {
        if (stream != null) {
            try {
                stream.close();
            } finally {
                stream = null;
            }
        }
    }

    /**
     * Get file object.
     *
     * @return the File being read.
     */
    public File getFile()
    {
        return file;
    }

    /**
     * Get the number of payload reader.
     *
     * @return number of payloads read
     */
    public int getNumberOfPayloads()
    {
        return nextNum;
    }

    /**
     * Read the next payload from the file.
     *
     * @throws PayloadException if there is a problem with the next payload
     */
    private void getNextPayload()
        throws PayloadException
    {
        gotNext = true;
        nextBuffer = readPayload();
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
                nextBuffer = null;
            }
        }

        return nextBuffer != null;
    }

    /**
     * This object is an iterator for itself.
     *
     * @return this object
     */
    public Iterator<ByteBuffer> iterator()
    {
        return this;
    }

    /**
     * Get the next available payload buffer.
     */
    public ByteBuffer next()
    {
        try {
            return nextBuffer();
        } catch (PayloadException pe) {
            LOG.error("Cannot return next payload buffer", pe);
            return null;
        }
    }

    /**
     * Get the next available payload buffer.
     *
     * @return next payload buffer (or <tt>null</tt>)
     *
     * @throws PayloadException if there is a problem with the next payload
     */
    public ByteBuffer nextBuffer()
        throws PayloadException
    {
        if (!gotNext) {
            getNextPayload();
        }

        gotNext = false;

        return nextBuffer;
    }

    /**
     * Read the next payload from the file.
     *
     * @return newly allocated ByteBuffer containing the payload
     *         or <tt>null</tt> at end of file
     *
     * @throws PayloadException if the payload could not be read
     */
    private ByteBuffer readPayload()
        throws PayloadException
    {
        // count this payload
        nextNum++;

        int len;
        try {
            len = stream.readInt();
        } catch (EOFException eof) {
            return null;
        } catch (IOException ioe) {
            throw new PayloadException("Couldn't read length of payload #" +
                                       nextNum + " in " + file, ioe);
        }

        // get payload length
        if (len < 4 || len > MAX_PAYLOAD_LEN) {
            throw new PayloadException("Bad length " + len + " for payload #" +
                                       nextNum + " in " + file);
        }

        // found stop message
        if (len == 4) {
            return null;
        }

        // Sender expects a separate buffer for each payload
        ByteBuffer buf;
        if (cache == null) {
            buf = ByteBuffer.allocate(len);
        } else {
            buf = cache.acquireBuffer(len);
        }

        buf.limit(len);
        buf.putInt(len);

        // read the rest of the payload
        for (int i = 0; i < 10 && buf.position() < buf.capacity(); i++) {
            int rtnval;
            try {
                rtnval = stream.read(buf.array(), buf.position(),
                                     len - buf.position());
            } catch (IOException ioe) {
                throw new PayloadException("Couldn't read " + len +
                                           " data bytes for payload #" +
                                           nextNum + " in " + file,
                                           ioe);
            }

            if (rtnval < 0) {
                throw new PayloadException("Reached end of file while " +
                                           " reading " + len +
                                           " data bytes for payload #" +
                                           nextNum + " in " +
                                           file);
            }

            // set the new buffer position
            buf.position(buf.position() + rtnval);
        }

        if (buf.position() < buf.capacity()) {
            final String msg =
                String.format("Got %d of %d bytes for payload #%d in %s",
                              buf.position(), buf.capacity(), nextNum, file);
            LOG.error(msg);
        }

        if (buf.position() != len) {
            final String msg =
                String.format("Expected to read %d bytes, not %d for" +
                              " payload #%d in %s", len, buf.position(),
                              nextNum, file);
            throw new PayloadException(msg);
        }

        if (len < 32) {
            final String msg =
                String.format("Got short payload #%d (%d bytes) in %s",
                              nextNum, len, file);
            throw new PayloadException(msg);
        }

        if (buf.getInt(0) == 32 && buf.getLong(24) == Long.MAX_VALUE) {
            final String msg =
                String.format("Found unexpected STOP message at" +
                              " payload #%d in %s", nextNum, file);
            throw new PayloadException(msg);
        }

        return buf;
    }

    /**
     * Unimplemented.
     */
    public void remove()
    {
        throw new Error("Unimplemented");
    }
}
