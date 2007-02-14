/**
 * SpliceablePayloadInputEngine
 * Date: Aug 5, 2005 2:31:12 PM
 *
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;
import icecube.daq.payload.IByteBufferCache;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.Set;
import java.nio.channels.SelectionKey;
import java.nio.channels.ReadableByteChannel;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is a Spliceable version of PayloadInputEngine class
 *
 * @author artur
 * @version $Id: SpliceablePayloadInputEngine.java,v 1.18 2006/02/08 11:28:02 toale Exp $
 */
public class SpliceablePayloadInputEngine extends PayloadInputEngine {

    private Splicer splicer;
    private SpliceableFactory spliceableFac;
    public int bytesCommittedToSplicer;
    // default maximum size of strand queue
    protected static final int DEFAULT_STRAND_QUEUE_MAX = 40000;

    private Log log = LogFactory.getLog(SpliceablePayloadInputEngine.class);

    public SpliceablePayloadInputEngine(String type,
                                        int id,
                                        String fcn,
                                        Splicer splicer,
                                        SpliceableFactory spliceableFac) {

        super(type, id, fcn);

        if (splicer == null) {
            throw new IllegalArgumentException("Splicer cannot be null");
        }
        this.splicer = splicer;
        if (spliceableFac == null) {
            throw new IllegalArgumentException("SpliceableFactory cannot be null");
        }
        this.spliceableFac = spliceableFac;
        // zero counter
        bytesCommittedToSplicer = 0;

    }

    public void startProcessing() {
        // zero counter
        bytesCommittedToSplicer = 0;

        while (splicer.getState() != Splicer.STOPPED) {
            if (log.isWarnEnabled()) {
                log.warn("Splicer should have been in STOPPED state. Calling Splicer.forceStop()");
            }
            splicer.forceStop();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                if (log.isErrorEnabled()) {
                    log.error("problem while sleeping: ", ie);
                }
            }
        }
        try {
            makeReverseConnections();
        } catch (IOException ioe) {
            throw new RuntimeException("Cannot make reverse connections", ioe);
        }

        if (splicer.getStrandCount() == 0) {
            Iterator rcvChanIter = rcvChanList.iterator();
            while (rcvChanIter.hasNext()) {
                SpliceablePayloadReceiveChannel payload = (SpliceablePayloadReceiveChannel) rcvChanIter.next();
                payload.setStrandTail(splicer.beginStrand());
            }
        } else {
            log.error("splicer should not any leftover strands-- count: " + splicer.getStrandCount());
        }
        splicer.start();
        super.startProcessing();
    }

    public synchronized Integer[] getStrandMax() {
        ArrayList strandMax = new ArrayList();
        Iterator rcvChanIter = rcvChanList.iterator();
        while (rcvChanIter.hasNext()) {
            SpliceablePayloadReceiveChannel msg =
                    (SpliceablePayloadReceiveChannel) rcvChanIter.next();
            strandMax.add(new Integer(msg.strandMax));
        }
        return (Integer[]) strandMax.toArray(new Integer[0]);
    }

    public synchronized void setAllStrandMax(int depth) {
        if (depth > 0) {
            Iterator rcvChanIter = rcvChanList.iterator();
            while (rcvChanIter.hasNext()) {
                SpliceablePayloadReceiveChannel msg =
                        (SpliceablePayloadReceiveChannel) rcvChanIter.next();
                msg.strandMax = depth;
            }
        }
    }

    public synchronized Integer[] getStrandDepth() {
        // lets make the negative to indicate a null strand end
        Integer depth = new Integer(-1);
        ArrayList strandDepth = new ArrayList();
        Iterator rcvChanIter = rcvChanList.iterator();
        while (rcvChanIter.hasNext()) {
            SpliceablePayloadReceiveChannel msg =
                    (SpliceablePayloadReceiveChannel) rcvChanIter.next();
            if (msg.strandTail != null) {
                depth = new Integer(msg.strandTail.size());
            }
            strandDepth.add(depth);
        }
        return (Integer[]) strandDepth.toArray(new Integer[0]);
    }

    public synchronized int getTotalStrandDepth() {
        int totalDepth = 0;
        Iterator rcvChanIter = rcvChanList.iterator();
        while (rcvChanIter.hasNext()) {
            SpliceablePayloadReceiveChannel msg =
                    (SpliceablePayloadReceiveChannel) rcvChanIter.next();
            if (msg.strandTail != null) {
                int depth = msg.strandTail.size();
                if (depth > 0) {
                    totalDepth += depth;
                }
            }
        }
        return totalDepth;
    }

    public synchronized int getBytesCommittedToSplicer() {
        return bytesCommittedToSplicer;
    }

    public synchronized Boolean[] getStrandFillingStopped() {
        ArrayList allocationStatus = new ArrayList();
        Iterator rcvChanIter = rcvChanList.iterator();
        while (rcvChanIter.hasNext()) {
            SpliceablePayloadReceiveChannel msg =
                    (SpliceablePayloadReceiveChannel) rcvChanIter.next();
            allocationStatus.add(new Boolean(msg.strandFillingStopped));
        }
        return (Boolean[]) allocationStatus.toArray(new Boolean[0]);
    }

    public boolean isHealthy() {
        boolean cacheHealthy = super.isHealthy();
        boolean strandWithOne = false;
        Integer[] strandDepths = getStrandDepth();
        for (int i = 0; i < strandDepths.length; i++) {
            if (1 == strandDepths[i].intValue()) {
                strandWithOne = true;
                break;
            }
        }
        return (cacheHealthy | !strandWithOne);
    }

    PayloadReceiveChannel createReceiveChannel(String name, ReadableByteChannel channel, IByteBufferCache bufMgr) {
        return new SpliceablePayloadReceiveChannel(name,
                        selector,
                        channel,
                        bufMgr,
                        inputAvailable,
                        spliceableFac);
    }
}
