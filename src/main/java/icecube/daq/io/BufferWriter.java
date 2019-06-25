package icecube.daq.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Write payloads to a file.
 */
public class BufferWriter
{
    /** Output channel */
    private FileChannel chan;

    /**
     * Write to the named file.
     *
     * @param name file name
     *
     * @throws IOException if the file cannot be opened
     */
    public BufferWriter(String name)
        throws IOException
    {
        this(new File(name), false);
    }

    /**
     * Write to the named file, appending if specified.
     *
     * @param name file name
     * @param append <tt>true</tt> if payloads should be appended to
     *               the existing file
     *
     * @throws IOException if the file cannot be opened
     */
    public BufferWriter(String name, boolean append)
        throws IOException
    {
        this(new File(name), append);
    }

    /**
     * Write to the specified file, appending if specified.
     *
     * @param file payload file
     * @param append <tt>true</tt> if payloads should be appended to
     *               the existing file
     *
     * @throws IOException if the file cannot be opened
     */
    public BufferWriter(File file, boolean append)
        throws IOException
    {
        chan = new FileOutputStream(file, append).getChannel();
    }

    /**
     * Close the file.
     *
     * @throws IOException if there was a problem closing the file
     */
    public void close()
        throws IOException
    {
        chan.close();
    }

    /**
     * Open a uniquely named file for writing.  If the specified name
     * is unavailable, a sequentially increasing number is appended
     * until a unique file name is found.
     *
     * @param baseName base filename
     *
     * @return handle to be used for writing buffers
     */
    public static final BufferWriter openNext(String baseName)
        throws IOException
    {
        String fileName = baseName;
        int num = 0;

        File file = null;
        while (true) {
            file = new File(fileName);
            if (!file.exists()) {
                break;
            }
            fileName = baseName + "." + num++;
        }

        return new BufferWriter(file, false);
    }

    /**
     * Write the buffer to the file.
     *
     * @throws IOException if there was a problem writing the buffer
     */
    public int write(ByteBuffer buf)
        throws IOException
    {
        return chan.write(buf);
    }

    /**
     * Write the buffer to the file, preserving the current buffer position.
     *
     * @param buf buffer to be written
     *
     * @throws IOException if there was a problem writing the buffer
     */
    public int writeAndRestore(ByteBuffer buf)
        throws IOException
    {
        return writeAndRestore(buf, 0);
    }

    /**
     * Write the buffer to the file, preserving the current buffer position.
     *
     * @param buf buffer to be written
     * @param offset buffer offset
     *
     * @throws IOException if there was a problem writing the buffer
     */
    public int writeAndRestore(ByteBuffer buf, int offset)
        throws IOException
    {
        if (buf == null) {
            throw new IOException("Cannot write null buffer");
        }

        final int pos = buf.position();
        buf.position(offset);
        int result;
        try {
            result = write(buf);
        } finally {
            buf.position(pos);
        }
        return result;
    }
}
