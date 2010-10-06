package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IWriteablePayload;
import icecube.icebucket.util.DiskUsage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Dispatch payload files to PnF
 */
public class FileDispatcher implements Dispatcher {
    public static final String DISPATCH_DEST_STORAGE = "/mnt/data/pdaqlocal";

    private static final Log LOG = LogFactory.getLog(FileDispatcher.class);

    private static final String START_PREFIX =
        DAQCmdInterface.DAQ_ONLINE_RUNSTART_FLAG;
    private static final String STOP_PREFIX =
        DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG;
    private static final String SUBRUN_START_PREFIX =
        DAQCmdInterface.DAQ_ONLINE_SUBRUNSTART_FLAG;
    private static final String CLOSE_PREFIX = "Close:";

    private static final long KB_IN_MB = 1024;

    private String baseFileName;
    private int numStarts;
    private WritableByteChannel outChannel;
    private IByteBufferCache bufferCache;
    private long totalDispatchedEvents;
    private int runNumber;
    private long maxFileSize = 10000000;
    private long currFileSize;
    private File tempFile;
    private String dispatchDestStorage;
    private int fileIndex;
    private long startingEventNum;
    private long diskSize;          // measured in MB
    private long diskAvailable;     // measured in MB

    public FileDispatcher(String baseFileName) {
        this(getDefaultDispatchDirectory(baseFileName), baseFileName, null);
    }

    public FileDispatcher(String baseFileName, IByteBufferCache bufferCache) {
        this(getDefaultDispatchDirectory(baseFileName), baseFileName,
             bufferCache);
    }

    public FileDispatcher(String destDir, String baseFileName)
    {
        this(destDir, baseFileName, null);
    }

    public FileDispatcher(String destDir, String baseFileName,
                          IByteBufferCache bufferCache)
    {
        setDispatchDestStorage(destDir, true);

        if (baseFileName == null){
            throw new IllegalArgumentException("baseFileName cannot be NULL!");
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        this.baseFileName = baseFileName;
        if (LOG.isInfoEnabled()) {
            LOG.info("baseFileName is set to: " + baseFileName);
        }
        if ( baseFileName.equalsIgnoreCase("tcal") ||
                   baseFileName.equalsIgnoreCase("sn")){
            maxFileSize = 200000000;
        }

        this.bufferCache = bufferCache;
    }

    /**
     * Close current file (if open)
     *
     * @throws DispatchException if there is a problem
     */
    public void close()
        throws DispatchException
    {
        dataBoundary(CLOSE_PREFIX);
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
                if (numStarts < 0) {
                    LOG.warn("Problem on receiving a STOP message --" +
                             " numStarts = " + numStarts);
                    numStarts = 0;
                }

                if (outChannel != null && outChannel.isOpen()) {
                    moveToDest();
                }
            }
        } else if (message.startsWith(SUBRUN_START_PREFIX) ||
                   message.startsWith(CLOSE_PREFIX))
        {
            if (outChannel != null && outChannel.isOpen()) {
                moveToDest();
            }
        } else {
            throw new DispatchException("Unknown dispatcher message: " +
                                        message);
        }
        checkDisk();
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
        if (tempFile == null) {
            tempFile = getTempFile(dispatchDestStorage, baseFileName);
            currFileSize = tempFile.length();
        }

        final boolean tempExists = tempFile.exists();

        if (!tempExists || outChannel == null || !outChannel.isOpen()) {
            FileOutputStream out;
            try {
                out = new FileOutputStream(tempFile.getPath());
            } catch (IOException ioe) {
                throw new DispatchException("Couldn't open " + tempFile, ioe);
            }
            currFileSize = tempFile.length();
            outChannel = out.getChannel();
            if (tempExists) {
                LOG.error("The last temp-" + baseFileName +
                          " file was not moved to the dispatch storage!!!");
            }
        }

        buffer.position(0);
        int numWritten;
        try {
            numWritten = outChannel.write(buffer);
            if (LOG.isDebugEnabled()) {
                LOG.debug("write ByteBuffer of length: " + buffer.limit() + " to file.");
            }
        } catch (IOException ioe) {
            throw new DispatchException(ioe);
        }

        if (numWritten != buffer.limit()) {
            LOG.error("Expected to write " + buffer.limit() + " bytes, not " +
                      numWritten);
        }
        ++totalDispatchedEvents;
        currFileSize += buffer.limit();

        if (currFileSize > maxFileSize) {
            moveToDest();
        }
    }

    /**
     * Dispatch a Payload event object
     *
     * @param event A payload object.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dispatchEvent(IWriteablePayload event)
        throws DispatchException {
        if (bufferCache == null) {
            final String errMsg =
                "Buffer cache is null! Cannot dispatch events!";

            throw new DispatchException(errMsg);
        }
        final int evtLen = event.getPayloadLength();
        ByteBuffer buffer = bufferCache.acquireBuffer(evtLen);
        int numWritten;
        try {
            numWritten = event.writePayload(false, 0, buffer);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new DispatchException("Couldn't write payload", ioe);
        }
        if (numWritten != evtLen) {
            throw new DispatchException("Expected payload to be " + evtLen +
                                        " bytes, but got " + numWritten);
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
     * Get the byte buffer cache being used.
     *
     * @return byte buffer cache
     */
    public IByteBufferCache getByteBufferCache()
    {
        return bufferCache;
    }

    /**
     * Get the default dispatch destination directory for the specified type.
     *
     * @param baseFileName base filename for the data stream
     *
     * @return destination directory
     *
     * @throws IllegalArgumentException if there is a problem
     */
    private static String getDefaultDispatchDirectory(String baseFileName)
    {
        if (baseFileName == null) {
            throw new IllegalArgumentException("baseFileName cannot be NULL!");
        }

        String dir;
        if (baseFileName.equalsIgnoreCase("physics")){
            dir = DISPATCH_DEST_STORAGE;
        } else if (baseFileName.equalsIgnoreCase("moni") ||
                   baseFileName.equalsIgnoreCase("tcal") ||
                   baseFileName.equalsIgnoreCase("sn")){
            // TODO: replace this later with the right directory
            dir = DISPATCH_DEST_STORAGE;
        } else {
            throw new IllegalArgumentException(baseFileName +
                                               " is unvalid name!");
        }

        return dir;
    }

    /**
     * Get the destination directory for completed data files.
     *
     * @return destination directory
     */
    public String getDispatchDestinationDirectory()
    {
        return dispatchDestStorage;
    }

    public static File getTempFile(String destDir, String baseFileName)
    {
        int extraNum = 0;
        String extraStr = "";

        File tmpFile;
        while (true) {
            tmpFile = new File(destDir, "temp-" + baseFileName + extraStr);
            if (!tmpFile.exists() || tmpFile.canWrite()) {
                break;
            }
            extraNum++;
            extraStr = "-" + extraNum;
        }
        return tmpFile;
    }

    public int getRunNumber()
    {
        return runNumber;
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
     * @param dirName The absolute path of directory where the dispatch files
     *        will be stored.
     */
    public void setDispatchDestStorage(String dirName)
    {
        setDispatchDestStorage(dirName, false);
    }

    /**
     * Set the destination directory where the dispatch files will be saved.
     *
     * @param dirName The absolute path of directory where the dispatch files
     *                will be stored.
     * @param fallback <tt>true</tt> if method should fall back to current
     *                 directory if specified directory is invalid
     */
    private void setDispatchDestStorage(String dirName, boolean fallback)
    {
        if (dirName == null){
            throw new IllegalArgumentException("destDir cannot be NULL!");
        }

        while (true) {
            File ddFile = new File(dirName);
            if (ddFile.exists() && ddFile.isDirectory()) {
                File testFile = new File(ddFile, "tempDispProbe");

                int nextNum = 1;
                while (testFile.exists()) {
                    testFile = new File(ddFile, "tempDispProbe" + nextNum++);
                }

                FileOutputStream out;

                boolean opened;
                try {
                    out = new FileOutputStream(testFile.getPath());
                    opened = true;
                } catch (FileNotFoundException fnfe) {
                    LOG.error("Cannot write to " + dirName + "!");
                    opened = false;
                    out = null;
                }

                if (opened) {
                    try {
                        out.close();
                    } catch (IOException ioe) {
                        // ignore close errors
                    }

                    testFile.delete();
                    break;
                }
            }

            final boolean isCurrentDir = dirName.equals(".");

            if (!fallback || isCurrentDir) {
                final String errMsg;

                if (isCurrentDir) {
                    errMsg = "Current directory does not exist!?!?!";
                } else {
                    errMsg = "\"" + dirName + "\" does not exist!?!?!";
                }

                throw new IllegalArgumentException(errMsg);
            }

            LOG.error(dirName + " does not exist!  Using current directory.");
            dirName = ".";
        }

        dispatchDestStorage = dirName;
        if (LOG.isInfoEnabled()) {
            LOG.info("dispatchDestStorage is set to: " + dispatchDestStorage);
        }

        if (tempFile != null) {
            LOG.error("dispatchDestStorage " + dispatchDestStorage +
                      " set after temp file " + tempFile + " was created");
        }
    }

    /**
     * Set the maximum size of the dispatch file.
     *
     * @param maxFileSize the maximum size of the dispatch file.
     */
    public void setMaxFileSize(long maxFileSize) {
        if (maxFileSize <= 0L) {
            throw new IllegalArgumentException("Bad maximum file size " +
                                               maxFileSize);
        }

        this.maxFileSize = maxFileSize;
    }

    /**
     * Returns the number of units still available in the disk (measured in MB).
     * If it fails to check the disk space, then it returns -1.
     *
     * @return the number of units still available in the disk.
     */
    public long getDiskAvailable(){
        return diskAvailable;
    }

    /**
     * Returns the total number of units in the disk (measured in MB).
     * If it fails to check the disk space, then it returns -1.
     *
     * @return the total number of units in the disk.
     */
    public long getDiskSize(){
        return diskSize;
    }

    private File getDestFile(){
        String fileName = baseFileName + "_" + runNumber + "_" + fileIndex +
            "_" + startingEventNum + "_" + + totalDispatchedEvents;
        File file = new File(dispatchDestStorage, fileName + ".dat");

        ++fileIndex;

        return file;
    }

    private void moveToDest() throws DispatchException {
        try {
            outChannel.close();
        } catch(IOException ioe){
            LOG.error("Problem when closing file channel: ", ioe);
            throw new DispatchException(ioe);
        }

        File destFile = getDestFile();
        if (!tempFile.exists()) {
            LOG.error("Couldn't move nonexistent temp file " + tempFile);
        } else if (!tempFile.renameTo(destFile)) {
            String errorMsg = "Couldn't move temp file " + tempFile +
                " to " + destFile;
            LOG.error(errorMsg);
            throw new DispatchException(errorMsg);
        }

        startingEventNum = totalDispatchedEvents + 1;

        checkDisk();
    }

    private void checkDisk(){
        DiskUsage usage = DiskUsage.getUsage(dispatchDestStorage);
        if (null == usage ||
            null == usage.getVolume()) {
            diskSize = -1;
            diskAvailable = -1;
            return;
        }
        diskSize = usage.getBlocks() / KB_IN_MB;
        diskAvailable = usage.getAvailable() / KB_IN_MB;
    }

    /**
     * A ShutdownHook for closing and renaming the dispatch file if it
     * is still open when invoked.
     */
    private class ShutdownHook extends Thread {
        public void run() {
            LOG.warn(" ShutdownHook invoked for " + baseFileName);
            if (outChannel != null && outChannel.isOpen()) {
                LOG.warn(" ShutdownHook: moving temp file for " + baseFileName);
                try {
                    moveToDest();
                } catch (DispatchException de) {
                    // We can't do anything about this now anyway...
                    LOG.error(" Problem in ShutdownHook for " + baseFileName + ": " + de);
                }
            }
        }
    }

    public String toString()
    {
        return "FileDispatcher[" + baseFileName + " starts " + numStarts +
            " run " + runNumber + " idx " + fileIndex +
            " totDisp " + totalDispatchedEvents + "]";
    }
}
