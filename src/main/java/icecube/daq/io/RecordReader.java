package icecube.daq.io;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple Class to read sequential records from
 * a TestDAQ file into a ByteBuffer.
 */
public class RecordReader 
{
    private String msFileName;
    private DataInputStream mtDataStream;
    private File mtFile;
    private FileInputStream mtFileInStream;

    public RecordReader(String sFileName) 
    {
        msFileName = "" + sFileName;
    }

    /**
     * Opens the file for reading records.
     */
    public void open() throws IOException 
    {
        mtFile = new File(msFileName);
        mtFileInStream = new FileInputStream(mtFile);
        mtDataStream = new DataInputStream(mtFileInStream);
    }

    /**
     * Closes any open streams.
     */
    public void close() throws IOException 
    {
        mtDataStream.close();
        mtFileInStream.close();
    }

    /**
     * Reads the next record into the current position into the ByteBuffer.
     * @param tBuffer ByteBuffer into which the raw record is read.
     * @return the length of the record read into the buffer.
     *                -1 if not enough room is left in the ByteBuffer.
     */
    public int readNextRecord(ByteBuffer tBuffer) 
        throws IOException 
    {
        int iStartPosition = tBuffer.position();
        int iLimit = tBuffer.limit();
        int iRecLen  = mtDataStream.readInt();
        int iBytesLeftInBuffer = iLimit - iStartPosition;
        if (iBytesLeftInBuffer < iRecLen) {
           return -1;
        }
        //-write the record length to the buffer
        tBuffer.putInt(iRecLen);
        if (tBuffer.hasArray()) {
            byte[] abBacking = tBuffer.array();
            mtDataStream.readFully(abBacking, iStartPosition + 4, iRecLen - 4);
        } else {
            for (int ii = 0; ii < (iRecLen - 4); ii++) {
                tBuffer.put(mtDataStream.readByte());
            }
        }
        return iRecLen;
    }

    /**
     * Reads the next record into the current position into the ByteBuffer.
     * @param iRecLen the length of the record to read into the ByteBuffer
     * @param tBuffer ByteBuffer into which the raw record is read.
     */
    public void readNextFixedLenghtRecord(int iRecLen, ByteBuffer tBuffer) 
        throws IOException 
    {
        int iStartPosition = tBuffer.position();
        if (tBuffer.hasArray()) {
            byte[] abBacking = tBuffer.array();
            mtDataStream.readFully(abBacking, iStartPosition, iRecLen);
        } else {
            for (int ii = 0; ii < iRecLen; ii++) {
                tBuffer.put(mtDataStream.readByte());
            }
        }
    }
}
