package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.payload.ISourceID;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.BasePayload;
import icecube.daq.payload.impl.DOMHit;
import icecube.daq.payload.impl.DOMHitFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Read payloads from a file.
 */
public class HitSpoolReader
    implements Iterator<ByteBuffer>, Iterable<ByteBuffer>
{
    private static final Log LOG = LogFactory.getLog(HitSpoolReader.class);

    /** Main file/directory */
    private File baseFile;
    /** hub ID */
    private int hubId;
    /** List of hitspool files */
    private ArrayList<IHitSpoolFile> files = new ArrayList<IHitSpoolFile>();
    /** Payload byte buffer reader */
    private PayloadByteReader rdr;

    /**
     * Open the named hitspool file or directory.
     *
     * @param name file name
     *
     * @throws IOException if the file cannot be opened
     */
    public HitSpoolReader(String name)
        throws IOException
    {
        this(name, Integer.MIN_VALUE);
    }

    /**
     * Open the named hitspool file or directory.
     *
     * @param name file name
     * @param hubId hub ID
     *
     * @throws IOException if the file cannot be opened
     */
    public HitSpoolReader(String name, int hubId)
        throws IOException
    {
        this(new File(name), hubId);
    }

    /**
     * Open the hitspool file or directory.
     *
     * @param baseFile payload file/directory
     *
     * @throws IOException if the file cannot be opened
     */
    public HitSpoolReader(File baseFile)
        throws IOException
    {
        this(baseFile, Integer.MIN_VALUE);
    }

    /**
     * Open the hitspool file or directory.
     *
     * @param baseFile payload file/directory
     * @param hubId hub ID
     *
     * @throws IOException if the file cannot be opened
     */
    public HitSpoolReader(File baseFile, int hubId)
        throws IOException
    {
        this.baseFile = baseFile;
        this.hubId = hubId;

        if (baseFile.isDirectory()) {
            findFiles(baseFile);

            // sort the list of hitspool files
            Collections.sort(files);
        } else if (baseFile.exists()) {
            try {
                files.add(new HitSpoolFile(baseFile, true));
            } catch (HitSpoolFileException hsfe) {
                throw new IOException("Cannot parse filename for " + baseFile);
            }
        }

        if (files.size() == 0) {
            throw new IOException("No files found for \"" + baseFile + "\"");
        }
    }

    /**
     * Close the file.
     *
     * @throws IOException if there was a problem closing the file
     */
    public void close()
        throws IOException
    {
        if (rdr != null) {
            try {
                rdr.close();
            } finally {
                rdr = null;
            }
        }

        files.clear();
    }

    /**
     * Find all hitspool files in the directory
     *
     * @param dir directory
     */
    private void findFiles(File dir)
    {
        File[] dirList = dir.listFiles();
        if (dirList == null) {
            return;
        }

        final String hubStr;
        final String fullStr;
        if (SourceIdRegistry.isIcetopHubSourceID(hubId)) {
            hubStr = null;
            fullStr = String.format("ithub%02d", hubId - 200);
        } else if (SourceIdRegistry.isIniceHubSourceID(hubId)) {
            hubStr = String.format("hub%02d", hubId);
            fullStr = String.format("ichub%02d", hubId);
        } else {
            hubStr = null;
            fullStr = null;
        }

        for (int i = 0; i < dirList.length; i++) {
            if (dirList[i].getName().equals("info.txt")) {
                continue;
            }

            // skip subdirectories
            if (dirList[i].isDirectory()) {
                continue;
            }

            // only match files for specified hub
            if (hubId > 0) {
                boolean matched = false;
                if (!matched && fullStr != null) {
                    matched = dirList[i].getName().startsWith(fullStr);
                }
                if (!matched && hubStr != null) {
                    matched = dirList[i].getName().startsWith(hubStr);
                }
                if (!matched) {
                    matched = dirList[i].getName().startsWith("HitSpool-") &&
                        dirList[i].getName().endsWith(".dat");
                }
                if (!matched) {
                    continue;
                }
            }

            try {
                files.add(new HitSpoolFile(dirList[i]));
            } catch (HitSpoolFileException hsfe) {
                LOG.error("Bad hit spool file " + dirList[i], hsfe);
            }
        }
    }

    /**
     * Get file object.
     *
     * @return the File being read.
     */
    public File getFile()
    {
        if (rdr == null) {
            return null;
        }

        return rdr.getFile();
    }

    /**
     * Get the number of payload reader.
     *
     * @return number of payloads read
     */
    public int getNumberOfPayloads()
    {
        if (rdr == null) {
            return 0;
        }

        return rdr.getNumberOfPayloads();
    }

    /**
     * Is another payload available?
     *
     * @return <tt>true</tt> if there is another payload
     */
    public boolean hasNext()
    {
        if (rdr == null) {
            if (files.size() == 0) {
                return false;
            }

            openNextFile();
            if (rdr == null) {
                return false;
            }
        }

        boolean val = rdr.hasNext();
        if (!val) {
            openNextFile();
            if (rdr == null) {
                return false;
            }

            val = rdr.hasNext();
        }

        return val;
    }

    /**
     * This object is an iterator for itself.
     *
     * @return this object
     */
    public Iterator iterator()
    {
        return this;
    }

    /**
     * Get the next available payload in a ByteBuffer.
     *
     * @return next payload (or <tt>null</tt>)
     */
    public ByteBuffer next()
    {
        try {
            return nextBuffer();
        } catch (PayloadException pe) {
            LOG.error("Cannot return next payload", pe);
            return null;
        }
    }

    /**
     * Get the next available payload in a ByteBuffer.
     *
     * @return next payload (or <tt>null</tt>)
     *
     * @throws PayloadException if there is a problem with the next payload
     */
    public ByteBuffer nextBuffer()
        throws PayloadException
    {
        if (!hasNext()) {
            return null;
        }

        return rdr.nextBuffer();
    }

    /**
     * Open the next file in the list
     */
    private void openNextFile()
    {
        if (rdr != null) {
            try {
                rdr.close();
            } catch (IOException ioe) {
                LOG.error("Cannot close " + rdr, ioe);
            }

            rdr = null;
        }

        while (files.size() > 0) {
            IHitSpoolFile hsf = files.remove(0);

            try {
                InputStream in = new FileInputStream(hsf.getFile());
                if (hsf.isGZipped()) {
                    in = new GZIPInputStream(in);
                }

                rdr = new PayloadByteReader(hsf.getFile(), in);
                break;
            } catch (IOException ioe) {
                LOG.error("Cannot open " + hsf.getFile(), ioe);
            }
        }
    }

    /**
     * Unimplemented.
     */
    public void remove()
    {
        throw new Error("Unimplemented");
    }

    enum ArgType { NONE, HUBNUM, MODULUS, NUM_TO_PRINT };

    public static void main(String[] args)
    {
        org.apache.log4j.BasicConfigurator.configure();

        boolean dumpHex = false;
        int hubNum = Integer.MIN_VALUE;
        int modulus = 1000;
        int numToPrint = Integer.MAX_VALUE;
        boolean verbose = false;
        ArrayList<String> files = new ArrayList<String>();

        ArgType parseType = ArgType.NONE;

        boolean usage = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].length() == 0) {
                System.err.println("Ignoring empty argument");
                continue;
            }

            if (parseType != ArgType.NONE) {
                try {
                    int val = Integer.parseInt(args[i]);
                    switch (parseType) {
                    case HUBNUM:
                        hubNum = val;
                        break;
                    case MODULUS:
                        modulus = val;
                        break;
                    case NUM_TO_PRINT:
                        numToPrint = val;
                        break;
                    default:
                        System.err.format("Found %d for unknown argument" +
                                          " type %s\n", val, parseType);
                        usage = true;
                        break;
                    }
                } catch (NumberFormatException nfe) {
                    System.err.format("Bad modulus value '%s'\n", args[i]);
                }

                parseType = ArgType.NONE;

                continue;
            }

            if (args[i].charAt(0) == '-') {
                if (args[i].length() == 1) {
                    System.err.println("Ignoring empty option '-'");
                } else {
                    if (args[i].charAt(1) == 'h') {
                        parseType = ArgType.HUBNUM;
                    } else if (args[i].charAt(1) == 'm') {
                        parseType = ArgType.MODULUS;
                    } else if (args[i].charAt(1) == 'n') {
                        parseType = ArgType.NUM_TO_PRINT;
                    } else if (args[i].charAt(1) == 'v') {
                        verbose = true;
                    } else if (args[i].charAt(1) == 'x') {
                        dumpHex = true;
                    } else {
                        System.err.format("Bad option '%s'\n", args[i]);
                        usage = true;
                        continue;
                    }

                    if (args[i].length() > 2) {
                        try {
                            int val = Integer.parseInt(args[i].substring(2));
                            switch (parseType) {
                            case HUBNUM:
                                hubNum = val;
                                break;
                            case MODULUS:
                                modulus = val;
                                break;
                            case NUM_TO_PRINT:
                                numToPrint = val;
                                break;
                            default:
                                System.err.format("Found %d for unknown" +
                                                  " argument type %s\n", val,
                                                  parseType);
                                break;
                            }
                        } catch (NumberFormatException nfe) {
                            System.err.format("Bad modulus value '%s'\n",
                                              args[i]);
                        }

                        parseType = ArgType.NONE;
                    }

                    continue;
                }

                continue;
            }

            files.add(args[i]);
        }

        if (usage) {
            System.err.printf("Usage: %s", HitSpoolReader.class.getName());
            System.err.printf(" [-h <hubNumber>]");
            System.err.printf(" [-m(odulus to count payloads)]");
            System.err.printf(" [-n <numberToPrint>]");
            System.err.printf(" [-v(erbose)]");
            System.err.printf(" [-x(dumpHex)]");
            System.err.printf(" file/directory ...");
            System.err.println();
            System.exit(1);
        }

        ISourceID sourceId = null;
        if (verbose) {
            final String compName = DAQCmdInterface.DAQ_STRING_HUB;

            sourceId = SourceIdRegistry.getISourceIDFromNameAndId(compName,
                                                                  hubNum);
        }

        for (String arg : files) {
            HitSpoolReader rdr;
            try {
                rdr = new HitSpoolReader(arg, hubNum);
            } catch (IOException ioe) {
                System.out.println("Cannot open " + arg);
                ioe.printStackTrace(System.out);
                continue;
            }

            int num = 0;
            for (ByteBuffer buf : rdr) {
                num++;
                if (num > numToPrint) {
                    break;
                }

                if (num % modulus == 0) {
                    if (!verbose) {
                        System.out.format("\r%d", num);
                    } else if (dumpHex) {
                        System.out.println(BasePayload.toHexString(buf, 0));
                    } else {
                        DOMHit tinyHit;
                        try {
                            tinyHit = DOMHitFactory.getHit(sourceId, buf, 0);
                            System.out.println(tinyHit.toString());
                        } catch (PayloadException pe) {
                            System.err.println("Couldn't get hit from buffer");
                            pe.printStackTrace();
                            tinyHit = null;
                        }
                    }
                }
            }

            try {
                rdr.close();
            } catch (IOException ioe) {
                System.out.println("Cannot close " + arg);
                ioe.printStackTrace(System.out);
            }

            if (hubNum > 0) {
                System.out.format("\rRead %d payloads for hub %d from %s\n",
                                  num, hubNum, arg);
            } else {
                System.out.format("\rRead %d payloads from %s\n",
                                  num, arg);
            }
        }
    }
}

interface IHitSpoolFile
    extends Comparable<IHitSpoolFile>
{
    File getFile();
    int getNumber();
    boolean isGZipped();
}

class HitSpoolFileException
    extends Exception
{
    HitSpoolFileException(String msg)
    {
        super(msg);
    }
}

class HitSpoolFile
    implements IHitSpoolFile
{
    private File f;
    private int num;

    HitSpoolFile(File f)
        throws HitSpoolFileException
    {
        this(f, true);
    }

    HitSpoolFile(File f, boolean assumeFirst)
        throws HitSpoolFileException
    {
        this.f = f;

        num = findFileNumber(assumeFirst);
    }

    public int compareTo(IHitSpoolFile hsf)
    {
        if (hsf == null) {
            return 1;
        }

        return getNumber() - hsf.getNumber();
    }

    public boolean equals(IHitSpoolFile hsf)
    {
        return compareTo(hsf) == 0;
    }

    private int findFileNumber(boolean assumeFirst)
        throws HitSpoolFileException
    {
        String name = getName();

        int end = name.length();
        if (name.indexOf(".gz", end - 3) == end - 3) {
            end -= 3;
        }
        if (name.indexOf(".dat", end - 4) == end - 4) {
            end -= 4;
        }

        int idx;
        for (idx = end;
             idx > 0 && Character.isDigit(name.charAt(idx - 1));
             idx--);

        if (idx == end) {
            if (assumeFirst) {
                return 1;
            }

            throw new HitSpoolFileException("Expected numbers before" +
                                           " \".dat\" at end of \"" +
                                           name + "\"");
        }

        String substr = name.substring(idx, end);
        try {
            return Integer.parseInt(substr);
        } catch (NumberFormatException nfe) {
            throw new HitSpoolFileException("Cannot extract number" +
                                           " from \"" + name + "\"");
        }
    }

    public File getFile()
    {
        return f;
    }

    public String getName()
    {
        return f.getName();
    }

    public int getNumber()
    {
        return num;
    }

    public int hashCode()
    {
        return f.hashCode();
    }

    public boolean isGZipped()
    {
        return f.getName().endsWith(".gz");
    }

    public String toString()
    {
        return f.toString();
    }
}
