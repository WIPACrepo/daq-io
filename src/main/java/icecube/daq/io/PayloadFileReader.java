package icecube.daq.io;

import icecube.daq.payload.MasterPayloadFactory;
import icecube.daq.payload.impl.PayloadEnvelope;
import icecube.daq.payload.splicer.Payload;
import icecube.daq.payload.splicer.PayloadFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;

/**
 * Simple Class to read sequential payloads which
 * are contained int PayloadEnvelopes into a ByteBuffer.
 */
public class PayloadFileReader {
    protected PayloadFactory mtPayloadFactory = new MasterPayloadFactory();
    protected String msFileName;
    protected DataInputStream mtDataStream;
    protected File mtFile;
    protected FileInputStream mtFileInStream;
    protected boolean mbIsOpen;

    public PayloadFileReader(String sFileName) {
        msFileName = ""+sFileName;
    }

    /**
     * Constructor which specifies the PayloadFactory to use for
     * creating payloads.
     * @param sFileName the name of the file to create payloads from.
     * @param tFactory PayloadFactory to use to create payloads.
     */
    public PayloadFileReader(String sFileName, PayloadFactory tFactory) {
        msFileName = ""+sFileName;
        mtPayloadFactory = tFactory;
    }

    /**
     * Returns the filename for this PayloadFileReader.
     * @return the filename.
     */
    public String getFileName() {
        return msFileName;
    }

    /**
     * Opens the file for reading records.
     */
    public void open() throws IOException {
        mtFile = new File(msFileName);
        mtFileInStream = new FileInputStream(mtFile);
        mtDataStream = new DataInputStream(mtFileInStream);
        mbIsOpen = true;
    }

    /**
     * checks to see if already open.
     */
    public boolean isOpen() {
        return mbIsOpen;
    }


    /**
     * Closes any open streams.
     */
    public void close() throws IOException {
        mtDataStream.close();
        mtFileInStream.close();
        mbIsOpen = false;
    }

    /**
     * Reads the next record into the current position into the ByteBuffer.
     * @param iOffset the offset from which to start this read.
     * @param tBuffer ByteBuffer into which the raw record is read.
     * @return the length of the record read into the buffer.
     *         -1 if not enough room is left in the ByteBuffer.
     * NTOE: This method positions the ByteBuffer to the next position
     *       past the payload that was read (ie startPosition + return value)
     *       if it is successful. Otherwise buffer position is unchanged.
     * @throws IOException if an error reading the data has occured
     *         EOFException if an attempt is made to read past end of stream.
     */
    public int readNextPayload(int iOffset, ByteBuffer tBuffer) throws IOException, DataFormatException {
        PayloadEnvelope tEnvelope = new PayloadEnvelope();
        int iStartPosition = iOffset;
        int iLimit = tBuffer.limit();
        int iBytesLeftInBuffer = iLimit - iStartPosition;
        if (iBytesLeftInBuffer < PayloadEnvelope.SIZE_ENVELOPE) return -1;
        //-Read the Envelope into the ByteBuffer first, then parse it
        for (int ii=0; ii < PayloadEnvelope.SIZE_ENVELOPE; ii++) {
            tBuffer.put(iStartPosition+ii, mtDataStream.readByte());
        }
        //-load the envelope from the ByteBuffer
        tEnvelope.loadData(iStartPosition, tBuffer);
        int iRecLen  = tEnvelope.miPayloadLen;
        if (iBytesLeftInBuffer < iRecLen) return -1;

        //-write the record length to the buffer

        int iPayloadOffset = PayloadEnvelope.SIZE_ENVELOPE;
        if (tBuffer.hasArray()) {
            byte[] abBacking = tBuffer.array();
            mtDataStream.readFully(abBacking, iStartPosition + iPayloadOffset, iRecLen - iPayloadOffset);
        } else {
            for (int ii = 0; ii < (iRecLen - iPayloadOffset); ii++) {
                tBuffer.put((iStartPosition + iPayloadOffset + ii), mtDataStream.readByte());
            }
        }
        return iRecLen;
    }
    /**
     * Reads the next record into the current position into the ByteBuffer.
     * @param tBuffer ByteBuffer into which the raw record is read.
     * @return the length of the record read into the buffer.
     *                -1 if not enough room is left in the ByteBuffer.
     * NTOE: This method positions the ByteBuffer to the next position
     *       past the payload that was read (ie startPosition + return value)
     *       if it is successful. Otherwise buffer position is unchanged.
     * @throws IOException if an error reading the data has occured
     *         EOFException if an attempt is made to read past end of stream.
     */
    public int readNextPayload(ByteBuffer tBuffer) throws IOException, DataFormatException {
        int iStartPosition = tBuffer.position();

        int iBytes = readNextPayload(iStartPosition, tBuffer);
        //-position ByteBuffer to end of read record
        tBuffer.position(iStartPosition + iBytes);
        return iBytes;
    }
    /**
     * Create's the next payload from the input stream source.
     * @param iOffset the offset into which to read and create the payload.
     * @param tBuffer ByteBuffer into which the next payload is to be read at the given offset.
     *
     * @return  Payload created by PayloadFactory after payload has been read
     *                     into the buffer starting at the given offset.
     *                     returns null if there is not enough room in the ByteBuffer to read
     *                     the payload.
     * NOTE: The length can be determined by position of buffer after read
     *       or from the Payload.getPayloadLength() attribute.
     * @exception EOFException is thrown to indicate that there are no more payloads in this source
     *                         and should be handled as a normal condition.
     * @exception IOException if there is an error condition.
     * @exception DataFormatException if there is an error condition.
     */
    public Payload createNextPayload(int iOffset, ByteBuffer tBuffer) throws IOException, DataFormatException  {
        readNextPayload(iOffset, tBuffer);
        return mtPayloadFactory.createPayload(iOffset, tBuffer);
    }

}
