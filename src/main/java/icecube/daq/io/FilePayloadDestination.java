package icecube.daq.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This object encapsulates the methods needed to create
 * an IPayloadDestination as a file.
 */
 public class FilePayloadDestination extends DataOutputStreamPayloadDestination {
    private static Log LOG = LogFactory.getLog(FilePayloadDestination.class);

    private String msFilename;
    private File mtFile;
    private FileOutputStream mtFileOutputStream;

    /**
     * Constructor.
     * @param sFilename String with the name of the file to be the destination.
     * @throws IOException if there is an error creating the destination.
     */
    public FilePayloadDestination(String sFilename) throws IOException {
        super();
        open(sFilename);
    }

    /**
     * Method to get the file name associated with this object.
     * @return the filename for this destination
     */
    public String getFileName() {
        return msFilename;
    }
    /**
     * Method to open the File as a destination.
     * @param sFilename....String with the filename to open.
     */
    private void open(String sFilename) throws IOException {
        msFilename = sFilename;
        mtFile = new File(msFilename);
        mtFileOutputStream = new FileOutputStream(mtFile);
        super.open(mtFileOutputStream);
        // mtDataStream = new DataInputStream(mtFileInStream);
    }

    /**
     * Closes the destination.
     */
    public void close() throws IOException {
        super.close();
        mtFileOutputStream.close();
        mtFileOutputStream = null;
        mtFile = null;
    }

    /**
     * Disposes of any used resources and closes.
     */
    public void dispose() {
        try {
            close();
        } catch (IOException tException) {
            LOG.error("Cannot close destination", tException);
        }
    }

 }
