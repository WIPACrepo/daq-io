package icecube.daq.io;

import icecube.daq.payload.IPayload;
import icecube.daq.payload.MasterPayloadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.zip.DataFormatException;

/**
 * Read payloads from a file.
 */
public class PayloadFileReader
    implements Iterable, Iterator
{
    /** Input channel */
    private ReadableByteChannel chan;
    /** Factory used to build payloads */
    private MasterPayloadFactory factory;
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
        this(new File(name));
    }

    /**
     * Open the file.
     *
     * @param file payload file
     *
     * @throws IOException if the file cannot be opened
     */
    public PayloadFileReader(File file)
        throws IOException
    {
        this(new FileInputStream(file));
    }

    /**
     * Use the specified stream to read payloads.
     *
     * @param stream payload file stream
     */
    public PayloadFileReader(FileInputStream stream)
    {
        this(stream.getChannel());
    }

    /**
     * Use the specified channel to read payloads.
     *
     * @param chan payload file channel
     */
    public PayloadFileReader(ReadableByteChannel chan)
    {
        this.chan = chan;

        factory = new MasterPayloadFactory();
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
        throws DataFormatException, IOException
    {
        if (lenBuf == null) {
            lenBuf = ByteBuffer.allocate(4);
        }

        gotNext = true;
        nextPayload = null;

        lenBuf.rewind();
        int numBytes = chan.read(lenBuf);
        if (numBytes >= 4) {
            int len = lenBuf.getInt(0);
            if (len < 4) {
                throw new DataFormatException("Bad length " + len);
            }

            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putInt(len);
            chan.read(buf);

            nextPayload = factory.createPayload(0, buf);
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
            } catch (DataFormatException dfe) {
                nextPayload = null;
            } catch (IOException ioe) {
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
        } catch (DataFormatException ioe) {
            return null;
        } catch (IOException ioe) {
            return null;
        }
    }

    /**
     * Get the next available payload.
     *
     * @return next payload (or <tt>null</tt>)
     *
     * @throws IOException if there is a problem with the next payload
     * @throws IOException if the next payload cannot be read
     */
    public IPayload nextPayload()
        throws DataFormatException, IOException
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
