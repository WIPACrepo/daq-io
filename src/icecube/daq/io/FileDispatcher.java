package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.splicer.Payload;
import icecube.daq.payload.IByteBufferCache;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileDispatcher implements Dispatcher {
    private static final Log LOG = LogFactory.getLog(FileDispatcher.class);

    private static final String START_PREFIX =
        DAQCmdInterface.DAQ_ONLINE_RUNSTART_FLAG;
    private static final String STOP_PREFIX =
        DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG;

    private String baseFileName;

    private int numStarts;
    private WritableByteChannel outChannel;
    private IByteBufferCache bufferCache;

    public FileDispatcher(String baseFileName){
        this(baseFileName, null);
    }

    public FileDispatcher(String baseFileName, IByteBufferCache bufferCache){
        this.baseFileName = baseFileName;
        this.bufferCache = bufferCache;
    }

    /**
     * Signals to the dispatch system that the set of events that preced this
     * call are separated, by some criteria, for those that succeed it.
     *
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dataBoundary()
            throws DispatchException
    {
        throw new DispatchException("dataBoundary() called with no argument");
    }

    /**
     * Signals to the dispatch system that the set of events that preced this
     * call are separated, by some criteria, for those that succeed it.
     * <p/>
     * The message supplied with this method is opaque to the system, i.e. it
     * is not used by the system, and it simple passed on through the any
     * delivery client.
     *
     * @param message a String explaining the reason for the boundary.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dataBoundary(String message)
        throws DispatchException
    {
        if (message == null) {
            final String errMsg =
                "dataBoundary() called with null argument";
            throw new DispatchException(errMsg);
        }

        String prefix = null;
        String errMsg = null;

        if (message.startsWith(START_PREFIX)) {
            numStarts++;
            prefix = START_PREFIX;
        } else if (message.startsWith(STOP_PREFIX)) {
            if (numStarts == 0) {
                errMsg = "FileDispatcher stopped while not running";
            } else {
                numStarts--;
            }
            prefix = STOP_PREFIX;
        } else {
            errMsg = "Unknown dispatcher message: " + message;
        }

        if (errMsg != null) {
            try {
                throw new Exception("StackTrace");
            } catch (Exception ex) {
                LOG.error(errMsg, ex);
            }
        } else if (prefix == null) {
            throw new Error("Programmer error: prefix should be set");
        } else {
            int runNum;
            try {
                runNum = Integer.parseInt(message.substring(prefix.length()));
            } catch (NumberFormatException nfe) {
                throw new Error("Couldn't parse run number from \"" +
                                message.substring(prefix.length()) + "\"");
            }

            if (prefix == STOP_PREFIX && numStarts == 0) {
                if (outChannel != null) {
                    try {
                        outChannel.close();
                    } catch (IOException ioe) {
                        throw new Error("Couldn't close output channel", ioe);
                    }

                    outChannel = null;
                }
            } else if (prefix == START_PREFIX) {
                if (outChannel == null) {
                    File fileName;
                    for (int i = 0; true; i++) {
                        fileName = new File(baseFileName + runNum + "." + i);
                        if (!fileName.exists()) {
                            break;
                        }
                    }

                    try {
                        FileOutputStream out =
                            new FileOutputStream(fileName.getPath());
                        outChannel = out.getChannel();
                    } catch (IOException ioe) {
                        throw new Error("Couldn't open dispatcher file", ioe);
                    }
                }
            }
        }
    }

    /**
     * Copies the event in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     *
     * @param buffer the ByteBuffer containg the event.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvent(ByteBuffer buffer) throws DispatchException {
        buffer.position(0);
        try {
            if (outChannel != null) {
                outChannel.write(buffer);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("write ByteBuffer of length: " + buffer.limit() + " to file.");
                }
            }
        } catch (IOException ioe) {
            if (LOG.isErrorEnabled()) {
                LOG.error("couldn't write to the file: ", ioe);
            }
            throw new DispatchException(ioe);
        }
    }

    /**
     * Dispatch a Payload event object
     *
     * @param event A payload object.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvent(Payload event) throws DispatchException {
        if (bufferCache == null){
            LOG.error("ByteBufferCache is null! Cannot dispatch events!");
            return;
        }
        ByteBuffer buffer = bufferCache.acquireBuffer(event.getPayloadLength());
        try {
            event.writePayload(false, 0, buffer);
        }catch(IOException ioe){
            throw new DispatchException(ioe);
        }
        dispatchEvent(buffer);
        bufferCache.returnBuffer(buffer);
    }

    /**
     * Copies the events in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     * <p/>
     * The number of events is taken to be the length of the indices array.
     *
     * @param buffer the ByteBuffer containg the events.
     * @param indices the 'position' of each event inside the buffer.
     * accepted.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvents(ByteBuffer buffer, int[] indices)
            throws DispatchException
    {
        throw new Error("Unimplemented");
    }

    /**
     * Copies the events in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     *
     * @param buffer the ByteBuffer containg the events.
     * @param indices the 'position' of each event inside the buffer.
     * @param count the number of events, this must be less that the length of
     * the indices array.
     * accepted.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvents(ByteBuffer buffer, int[] indices, int count)
            throws DispatchException
    {
        throw new Error("Unimplemented");
    }
}
