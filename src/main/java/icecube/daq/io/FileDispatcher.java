package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IWriteablePayload;

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

    private static final long BYTES_IN_MB = 1024 * 1024;

    /** Avoid multiple warnings for unusual base names */
    private static boolean warnedName;

    private String baseFileName;
    private int numStarts;
    private WritableByteChannel outChannel;
    private IByteBufferCache bufferCache;
    private long numDispatchedEvents;
    private long totalDispatchedEvents;
    private int runNumber;
    private long maxFileSize = 10000000;
    private long currFileSize;
    private long numBytesWritten;
    private File tempFile;
    private Object fileLock = new Object();
    private File dispatchDir;
    private int fileIndex;
    private long startingEventNum;
    private long diskSize;          // measured in MB
    private long diskAvailable;     // measured in MB

    public FileDispatcher(String baseFileName) {
        this(null, baseFileName, null);
    }

    public FileDispatcher(String baseFileName, IByteBufferCache bufferCache) {
        this(null, baseFileName, bufferCache);
    }

    public FileDispatcher(String destDir, String baseFileName)
    {
        this(destDir, baseFileName, null);
    }

    public FileDispatcher(String destDir, String baseFileName,
                          IByteBufferCache bufferCache)
    {
        if (destDir != null) {
            setDispatchDestStorage(destDir, true);
        }

        if (baseFileName == null) {
            throw new IllegalArgumentException("baseFileName cannot be NULL!");
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        this.baseFileName = baseFileName;
        if (LOG.isInfoEnabled()) {
            LOG.info("baseFileName is set to: " + baseFileName);
        }
        if (baseFileName.equalsIgnoreCase("tcal") ||
            baseFileName.equalsIgnoreCase("sn"))
        {
            maxFileSize = 200000000;
        }

        this.bufferCache = bufferCache;

        this.numBytesWritten=0;
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
     * <p>
     * The message supplied with this method is opaque to the system, i.e. it
     * is not used by the system, and it simple passed on through the any
     * delivery client.
     *
     * @param message a String explaining the reason for the boundary.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    public void dataBoundary(String message) throws DispatchException
    {
        if (message == null) {
            throw new DispatchException("dataBoundary() called with null" +
                                        " argument!");
        }

        if (dispatchDir == null) {
            String dirName = getDefaultDispatchDirectory(baseFileName);
            setDispatchDestStorage(dirName, true);
        }

        if (message.startsWith(START_PREFIX)) {
            String runStr = message.substring(START_PREFIX.length());
            try {
                runNumber = Integer.parseInt(runStr);
            } catch (java.lang.NumberFormatException nfe) {
                throw new DispatchException("Cannot start run;" +
                                            " bad run number \"" + runStr +
                                            "\"");
            }

            startDispatch();
            ++numStarts;
        } else if (message.startsWith(STOP_PREFIX)) {
            if (numStarts == 0) {
                throw new DispatchException("FileDispatcher stopped while" +
                                            " not running!");
            } else {
                numStarts--;
                if (numStarts < 0) {
                    LOG.warn("Problem on receiving a STOP message --" +
                             " numStarts = " + numStarts);
                    numStarts = 0;
                }

                moveToDest();
            }
        } else if (message.startsWith(SUBRUN_START_PREFIX) ||
                   message.startsWith(CLOSE_PREFIX))
        {
            moveToDest();
        } else if (message.startsWith(SWITCH_PREFIX)) {
            String runStr = message.substring(SWITCH_PREFIX.length());

            int newNumber;
            try {
                newNumber = Integer.parseInt(runStr);
            } catch (java.lang.NumberFormatException nfe) {
                throw new DispatchException("Cannot switch run;" +
                                            " bad run number \"" + runStr +
                                            "\"");
            }

            synchronized (fileLock) {
                moveToDest();
                runNumber = newNumber;
                startDispatch();
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
        synchronized (fileLock) {
            if (tempFile == null) {
                tempFile = getTempFile(dispatchDir, baseFileName);
                currFileSize = tempFile.length();
            }

            final boolean tempExists = tempFile.exists();

            if (!tempExists || outChannel == null || !outChannel.isOpen()) {
                outChannel = openFile(tempFile);
                currFileSize = tempFile.length();
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
                    LOG.debug("write ByteBuffer of length: " + buffer.limit() +
                              " to file.");
                }
            } catch (IOException ioe) {
                throw new DispatchException(ioe);
            }

            if (numWritten != buffer.limit()) {
                LOG.error("Expected to write " + buffer.limit() +
                          " bytes, not " + numWritten);
            }
        }

        ++numDispatchedEvents;
        ++totalDispatchedEvents;
        currFileSize += buffer.limit();
        numBytesWritten += buffer.limit();

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
        final int evtLen = event.length();
        ByteBuffer buffer = bufferCache.acquireBuffer(evtLen);
        int numWritten;
        try {
            numWritten = event.writePayload(false, 0, buffer);
        } catch (IOException ioe) {
            throw new DispatchException("Couldn't write payload " + event, ioe);
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
     * <p>
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

        String dir = DISPATCH_DEST_STORAGE;
        if (!warnedName &&
            !baseFileName.equalsIgnoreCase("physics") &&
            !baseFileName.equalsIgnoreCase("moni") &&
            !baseFileName.equalsIgnoreCase("tcal") &&
            !baseFileName.equalsIgnoreCase("sn"))
        {
            LOG.error("Dispatching to unusual base name " + baseFileName);
            warnedName = true;
        }

        return dir;
    }

    /**
     * Get the destination directory where the dispatch files will be saved.
     *
     * @return The absolute path where the dispatch files will be stored.
     */
    public File getDispatchDestStorage()
    {
        return dispatchDir;
    }

    public static File getTempFile(String destDirName, String baseFileName)
        throws DispatchException
    {
        return getTempFile(new File(destDirName), baseFileName);
    }

    public static File getTempFile(File destDir, String baseFileName)
        throws DispatchException
    {
        if (!destDir.exists()) {
            throw new DispatchException("Destination directory \"" + destDir +
                                        "\" does not exist");
        }

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
     * Get the number of bytes written to disk
     *
     * @return a long value ( number of bytes written to disk )
     */
    public long getNumBytesWritten() {
        return numBytesWritten;
    }

    /**
     * Get the  number of events dispatched during this run
     * @return a long value
     */
    public long getNumDispatchedEvents() {
        return numDispatchedEvents;
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
     * Does this dispatcher have one or more active STARTs?
     *
     * @return <tt>true</tt> if dispatcher has been started and not stopped
     */
    public boolean isStarted()
    {
        return numStarts > 0;
    }

    public WritableByteChannel openFile(File file)
        throws DispatchException
    {
        FileOutputStream out;
        try {
            out = new FileOutputStream(file.getPath());
        } catch (IOException ioe) {
            throw new DispatchException("Couldn't open " + file, ioe);
        }
        return out.getChannel();
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

        final String origName = dirName;
        while (true) {
            File ddFile = new File(dirName);
            if (ddFile.isDirectory() && ddFile.canWrite()) {
                // writing and reading a file is probably overly paranoid but
                //  doesn't really hurt anything, so better safe than sorry
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

                    if(!testFile.delete()) {
                        LOG.info("deleting: "+testFile.getPath()+" failed");
                    }
                    break;
                }
            }

            if (!fallback || dirName.equals(".")) {
                final String errMsg;

                if (dirName.equals(".")) {
                    errMsg = "Current directory does not exist!?!?!";
                } else {
                    errMsg = "\"" + dirName + "\" does not exist!?!?!";
                }

                throw new IllegalArgumentException(errMsg);
            }

            dirName = ".";
        }

        if (dirName.equals(".") && !dirName.equals(origName)) {
            LOG.error(origName + " does not exist or is not writable!" +
                      "  Using current directory.");
        }

        dispatchDir = new File(dirName);
        if (LOG.isInfoEnabled()) {
            LOG.info("dispatchDestStorage is set to: " + dispatchDir);
        }

        if (tempFile != null) {
            LOG.error("dispatchDestStorage " + dispatchDir +
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
        final String fileName =
            String.format("%s_%06d_%06d_%d_%d.dat", baseFileName, runNumber,
                          fileIndex++, startingEventNum, numDispatchedEvents);
        return new File(dispatchDir, fileName);
    }

    private void moveToDest() throws DispatchException {
        if (outChannel == null || !outChannel.isOpen()) {
            return;
        }

        synchronized (fileLock) {
            try {
                outChannel.close();
            } catch(IOException ioe){
                LOG.error("Problem when closing file channel: ", ioe);
                throw new DispatchException(ioe);
            }

            File destFile = getDestFile();
            if (!tempFile.exists()) {
                LOG.error("Couldn't move nonexistent temp file " + tempFile);
            } else if (destFile.exists()) {
                String errorMsg = "Couldn't overwrite existing " + destFile +
                    " with temp file " + tempFile;
                throw new DispatchException(errorMsg);
            } else if (!tempFile.renameTo(destFile)) {
                String errorMsg = "Couldn't move temp file " + tempFile +
                    " to " + destFile;
                throw new DispatchException(errorMsg);
            }

            startingEventNum = numDispatchedEvents + 1;
        }

        checkDisk();
    }

    private void checkDisk(){
        if (!dispatchDir.exists()) {
            // can't check disk if dispatch directory doesn't exist
            diskSize = -1;
            diskAvailable = -1;
            return;
        }

        diskSize = dispatchDir.getTotalSpace() / BYTES_IN_MB;
        diskAvailable = dispatchDir.getUsableSpace() / BYTES_IN_MB;
    }

    private void startDispatch()
    {
        numDispatchedEvents = 0;
        startingEventNum = 0;
        fileIndex = 0;
    }

    /**
     * A ShutdownHook for closing and renaming the dispatch file if it
     * is still open when invoked.
     */
    private class ShutdownHook extends Thread {
        public void run() {
            LOG.debug("ShutdownHook invoked for " + baseFileName);
            if (outChannel != null && outChannel.isOpen()) {
                LOG.warn("ShutdownHook: moving temp file for " + baseFileName);
                try {
                    moveToDest();
                } catch (DispatchException de) {
                    // We can't do anything about this now anyway...
                    LOG.error("Problem in ShutdownHook for " + baseFileName +
                              ": " + de);
                }
            }
        }
    }

    public String toString()
    {
        return "FileDispatcher[" + baseFileName + " starts " + numStarts +
            " run " + runNumber + " idx " + fileIndex +
            " numDisp " + numDispatchedEvents +
            " totDisp " + totalDispatchedEvents + "]";
    }
}
