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
     * Write the buffer to the file.
     *
     * @throws IOException if there was a problem writing the buffer
     */
    public int write(ByteBuffer buf)
        throws IOException
    {
        return chan.write(buf);
    }
}
