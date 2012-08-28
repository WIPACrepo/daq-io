package icecube.daq.io;

import icecube.daq.payload.PayloadException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Read payload bytes from a file.
 */
public class PayloadByteReader
    implements Iterator<ByteBuffer>
{
    private static final Log LOG = LogFactory.getLog(PayloadByteReader.class);

    /** maximum payload length (to avoid garbage inputs) */
    private static final int MAX_PAYLOAD_LEN = 10000000;

    /** Input file */
    private File file;
    /** Input channel */
    private ReadableByteChannel chan;
    /** ByteBuffer used to read the payload length */
    private ByteBuffer lenBuf;

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
        this.file = file;

        FileInputStream stream = new FileInputStream(file);
        chan = stream.getChannel();
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
        if (lenBuf == null) {
            lenBuf = ByteBuffer.allocate(4);
        } else {
            lenBuf.rewind();
        }

        // count this payload
        nextNum++;

        int numBytes;
        try {
            numBytes = chan.read(lenBuf);
        } catch (IOException ioe) {
            throw new PayloadException("Couldn't read length of payload #" +
                                       nextNum + " in " + file, ioe);
        }

        if (numBytes < 0) {
            // return null at end of file
            return null;
        }

        if (numBytes < 4) {
            throw new PayloadException("Only got " + numBytes +
                                       " of length for payload #" +
                                       nextNum + " in " + file);
        }

        // get payload length
        int len = lenBuf.getInt(0);
        if (len < 4 || len > MAX_PAYLOAD_LEN) {
            throw new PayloadException("Bad length " + len + " for payload #" +
                                       nextNum + " in " + file);
        }

        // Sender expects a separate buffer for each payload
        ByteBuffer buf = ByteBuffer.allocate(len);
        buf.limit(len);
        buf.putInt(len);

        // read the rest of the payload
        for (int i = 0; i < 10 && buf.position() < buf.capacity(); i++) {
            int rtnval;
            try {
                rtnval = chan.read(buf);
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

            if (buf.position() < buf.capacity()) {
                LOG.error("Partial read of payload #" + nextNum +
                          " in " + file);
            }
        }

        if (buf.position() != len) {
            throw new PayloadException("Expected to read " + len +
                                       " bytes, not " + buf.position() +
                                       " for payload #" + nextNum +
                                       " in " + file);
        }

        if (len < 32) {
            throw new PayloadException("Got short payload #" + nextNum +
                                       " (" + len + " bytes) in " + file);
        }

        if (buf.getInt(0) == 32 && buf.getLong(24) == Long.MAX_VALUE) {
            throw new PayloadException("Found unexpected STOP message " +
                                       " at payload #" + nextNum +
                                       " in " + file);
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
