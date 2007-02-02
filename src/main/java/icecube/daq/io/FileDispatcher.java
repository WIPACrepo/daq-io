package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.splicer.Payload;
import icecube.daq.payload.IByteBufferCache;
import icecube.icebucket.util.DiskUsage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Dispatch payload files to PnF
 */
public class FileDispatcher implements Dispatcher {
    private static final Log LOG = LogFactory.getLog(FileDispatcher.class);

    private static final String START_PREFIX =
        DAQCmdInterface.DAQ_ONLINE_RUNSTART_FLAG;
    private static final String STOP_PREFIX =
        DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG;
    private static final String DISPATCH_DEST_STORAGE = "/mnt/data/pdaqlocal";
    private static final int KB_IN_MB = 1024;

    private String baseFileName;
    private int numStarts;
    private WritableByteChannel outChannel;
    private IByteBufferCache bufferCache;
    private long totalDispatchedEvents;
    private int runNumber;
    private long maxFileSize = 10000000;
    private File tempFile;
    private String dispatchDestStorage;
    private int fileIndex;
    private FilenameFilter filter;
    private long startingEventNum;
    private int diskSize;          // measured in MB
    private int diskAvailable;     // measured in MB

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

        this.baseFileName = baseFileName;
        LOG.info("baseFileName is set to: " + baseFileName);

        this.bufferCache = bufferCache;

        tempFile = getTempFile(dispatchDestStorage, baseFileName);

        filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".dat");
            }
        };
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

                fileIndex = 0;
                startingEventNum = 0;
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
        final boolean tempExists = tempFile.exists();

        if (!tempExists || outChannel == null || !outChannel.isOpen()) {
            try {
                FileOutputStream out = new FileOutputStream(tempFile.getPath());
                outChannel = out.getChannel();
            } catch (IOException ioe) {
                LOG.error("couldn't initiate temp dispatch file ", ioe);
                throw new DispatchException(ioe);
            }
            if (tempExists) {
                LOG.error("The last temp-" + baseFileName +
                          " file was not moved to the dispatch storage!!!");
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
            final String errMsg =
                "ByteBufferCache is null! Cannot dispatch events!";

            throw new DispatchException(errMsg);
        }
        ByteBuffer buffer = bufferCache.acquireBuffer(event.getPayloadLength());
        try {
            event.writePayload(false, 0, buffer);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new DispatchException("Couldn't write payload", ioe);
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
            dir = ".";
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
        return new File(destDir, "temp-" + baseFileName);
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
        LOG.info("dispatchDestStorage is set to: " + dispatchDestStorage);
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
    public int getDiskAvailable(){
        return diskAvailable;
    }

    /**
     * Returns the total number of units in the disk (measured in MB).
     * If it fails to check the disk space, then it returns -1.
     *
     * @return the total number of units in the disk.
     */
    public int getDiskSize(){
        return diskSize;
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
        } catch(IOException ioe){
            LOG.error("Problem when closing file channel: ", ioe);
            throw new DispatchException(ioe);
        }

        File destFile = getDestFile();
        if (!tempFile.renameTo(destFile)) {
            String errorMsg = "Couldn't move temp file to " + destFile;
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
        diskSize = (int)usage.getBlocks() / KB_IN_MB;
        diskAvailable = (int) usage.getAvailable() / KB_IN_MB;
    }
}
