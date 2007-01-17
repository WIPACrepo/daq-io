package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.splicer.Payload;
import icecube.daq.payload.IByteBufferCache;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FilenameFilter;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileDispatcher implements Dispatcher {
    private static final Log LOG = LogFactory.getLog(FileDispatcher.class);

    private static final String START_PREFIX = DAQCmdInterface.DAQ_ONLINE_RUNSTART_FLAG;
    private static final String STOP_PREFIX = DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG;

    private String baseFileName;
    private int numStarts;
    private WritableByteChannel outChannel;
    private IByteBufferCache bufferCache;
    private long totalDispatchedEvents;
    private int runNumber;
    private long maxFileSize = 10000000;
    private File tempFile;
    private String dispatchDestStorage = ".";
    private int fileIndex;
    private FilenameFilter filter;
    private long startingEventNum;

    public FileDispatcher(String baseFileName) {
        this(baseFileName, null);
    }

    public FileDispatcher(String baseFileName, IByteBufferCache bufferCache) {
        this.baseFileName = baseFileName;
        this.bufferCache = bufferCache;
        tempFile = new File("temp-" + baseFileName);
        filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".dat");
            }
        };
        LOG.info("creating FileDispatcher for " + baseFileName);
    }

    /**
     * Signals to the dispatch system that the set of events that preced this
     * call are separated, by some criteria, for those that succeed it.
     *
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dataBoundary()
            throws DispatchException {
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
    public void dataBoundary(String message) throws DispatchException {

        if (message == null) {
            throw new DispatchException("dataBoundary() called with null argument!");
        }

        if (message.startsWith(START_PREFIX)) {
            totalDispatchedEvents = 0;
            startingEventNum = 0;
            ++numStarts;
            runNumber = Integer.parseInt(message.substring(START_PREFIX.length()));
            fileIndex = 0;
        } else if (message.startsWith(STOP_PREFIX)) {
            if (numStarts == 0) {
                throw new DispatchException("FileDispatcher stopped while not running!");
            } else {
                numStarts--;
                if (numStarts != 0) {
                    LOG.warn("Problem on receiving a STOP message -- numStarts = " + numStarts);
                } else {
                    if (outChannel != null && outChannel.isOpen()) {
                        moveToDest();
                    }
                }
                fileIndex = 0;
                startingEventNum = 0;
                return;
            }
        } else {
            throw new DispatchException("Unknown dispatcher message: " + message);
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
        if (!tempFile.exists()) {
            try {
                FileOutputStream out = new FileOutputStream(tempFile.getPath());
                outChannel = out.getChannel();
            } catch (IOException ioe) {
                LOG.error("couldn't initiate temp dispatch file ", ioe);
                throw new DispatchException(ioe);
            }
        } else {
            if (outChannel == null || !outChannel.isOpen()){
                String tempFileName = "temp-" + baseFileName;
                int i = 0;
                while(true){
                    String name = "temp-" + baseFileName + "_" + runNumber + "_" + i;
                    File tmp = new File(name);
                    if (!tmp.exists()){
                        tempFile.renameTo(tmp);
                        break;
                    }
                    ++i;
                }

                tempFile = new File(tempFileName);
                try {
                    FileOutputStream out = new FileOutputStream(tempFile.getPath());
                    outChannel = out.getChannel();
                } catch(IOException ioe){
                    LOG.error(ioe.getMessage(), ioe);
                    throw new DispatchException(ioe);
                }
                LOG.error("The last temp-" + baseFileName + " file was not moved to the dispatch storage!!!");
            }
        }
        buffer.position(0);
        try {
            outChannel.write(buffer);
            if (LOG.isDebugEnabled()) {
                LOG.debug("write ByteBuffer of length: " + buffer.limit() + " to file.");
            }
        } catch (IOException ioe) {
            LOG.error("couldn't write to the file: ", ioe);
            throw new DispatchException(ioe);
        }
        ++totalDispatchedEvents;

        if (tempFile.length() > maxFileSize) {
            moveToDest();
        }
    }

    /**
     * Dispatch a Payload event object
     *
     * @param event A payload object.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvent(Payload event) throws DispatchException {
        if (bufferCache == null) {
            LOG.error("ByteBufferCache is null! Cannot dispatch events!");
            return;
        }
        ByteBuffer buffer = bufferCache.acquireBuffer(event.getPayloadLength());
        try {
            event.writePayload(false, 0, buffer);
        } catch (IOException ioe) {
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
     * @param buffer  the ByteBuffer containg the events.
     * @param indices the 'position' of each event inside the buffer.
     *                accepted.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvents(ByteBuffer buffer, int[] indices)
            throws DispatchException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    /**
     * Copies the events in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     *
     * @param buffer  the ByteBuffer containg the events.
     * @param indices the 'position' of each event inside the buffer.
     * @param count   the number of events, this must be less that the length of
     *                the indices array.
     *                accepted.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvents(ByteBuffer buffer, int[] indices, int count)
            throws DispatchException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    /**
     * Get the total of the dispatched events
     *
     * @return a long value
     */
    public long getTotalDispatchedEvents() {
        return totalDispatchedEvents;
    }

    /**
     * Set the destination directory where the dispatch files will be saved.
     *
     * @param dirName The absolute path of directory where the dispatch files will be stored.
     */
    public void setDispatchDestStorage(String dirName) {
        dispatchDestStorage = dirName;
        File destDirectory = new File(dispatchDestStorage);
        if (!destDirectory.exists() || !destDirectory.isDirectory()){
            LOG.error(dispatchDestStorage + " is not a directory!");
            throw new IllegalArgumentException(dispatchDestStorage + " is not a directory!");
        }
    }

    /**
     * Set the maximum size of the dispatch file.
     *
     * @param maxFileSize the maximum size of the dispatch file.
     */
    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    private File getDestFile(){

        File destDispatchStorage = new File(dispatchDestStorage);

        String[] fileNames = destDispatchStorage.list(filter);
        String runNumberStr = String.valueOf(runNumber);
        StringBuffer sbBaseFileName = new StringBuffer(baseFileName);
        StringBuffer sbRunNum = new StringBuffer(runNumberStr);
        List oldDispatchFiles = new ArrayList();
        for (int i = 0; i < fileNames.length; i++){
            if (fileNames[i].contains(sbBaseFileName) && fileNames[i].contains(sbRunNum)){
                oldDispatchFiles.add(fileNames[i]);
            }
        }

        int tmpIndex = 0;
        if (oldDispatchFiles.size() > 0){
            List indices = new ArrayList();
            for (int i = 0; i < oldDispatchFiles.size(); i++){
                String name = (String)oldDispatchFiles.get(i);
                StringTokenizer st = new StringTokenizer(name, "_");
                int y = 0;
                while(st.hasMoreTokens()){
                    String tempName = st.nextToken();
                    if (y == 2){
                        indices.add(new Integer(tempName));
                        break;
                    }
                    ++y;
                }
            }
            Collections.sort(indices);
            Integer last = (Integer)indices.get(indices.size() -1);
            tmpIndex = last.intValue() + 1;
        } else {
            tmpIndex = fileIndex;
            ++fileIndex;
        }


        String fileName = baseFileName + "_" + runNumber + "_" + tmpIndex + "_" +
                startingEventNum + "_" + totalDispatchedEvents;

        File file = new File(dispatchDestStorage, fileName + ".dat");

        return file;
    }

    private void moveToDest() throws DispatchException {
        try {
            outChannel.close();
            if (!tempFile.renameTo(getDestFile())) {
                String errorMsg = "Couldn't move temp file to " + getDestFile();
                LOG.error(errorMsg);
                throw new DispatchException(errorMsg);
            }
            //++fileIndex;
            startingEventNum = totalDispatchedEvents + 1;
        } catch(IOException ioe){
            LOG.error("Problem when closing file channel: ", ioe);
            throw new DispatchException(ioe);
        }
    }
}
