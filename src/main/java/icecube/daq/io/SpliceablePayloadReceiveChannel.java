/**
 * SpliceablePayloadReceiveChannel
 * Date: Aug 5, 2005 11:26:08 AM
 *
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

import icecube.daq.splicer.*;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.common.NormalState;

import java.nio.channels.Selector;
import java.nio.channels.ReadableByteChannel;

import EDU.oswego.cs.dl.util.concurrent.Semaphore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is the spliceable version of receive channel.
 *
 * @author artur
 * @version $Id: SpliceablePayloadReceiveChannel.java,v 1.21 2006/02/06 16:31:13 jboss Exp $
 */
public class SpliceablePayloadReceiveChannel extends PayloadReceiveChannel {

    private SpliceableFactory spliceableFac;
    protected StrandTail strandTail;
    protected boolean strandFillingStopped = false;
    protected int strandMax = SpliceablePayloadInputEngine.DEFAULT_STRAND_QUEUE_MAX;

    private Log log = LogFactory.getLog(SpliceablePayloadReceiveChannel.class);

    public SpliceablePayloadReceiveChannel(String myID,
                                           Selector sel,
                                           ReadableByteChannel channel,
                                           IByteBufferCache bufMgr,
                                           Semaphore inputSem,
                                           SpliceableFactory spliceableFac) {

        super(myID, sel, channel, bufMgr, inputSem);

        if (spliceableFac == null) {
            throw new IllegalArgumentException("SpliceableFactory cannot be null");
        }
        this.spliceableFac = spliceableFac;
    }

    protected void exitIdle() {
        super.exitIdle();
        if (strandTail == null) {
            // just to be paranoid, check that strandTail has
            // been initialized before continuing.
            doTransition(SIG_ERROR, STATE_ERROR);
            enterError();
        }
    }

    protected void exitRecvHeader() {
        if (inputBuf.getInt(bufPos) == INT_SIZE) {
            notifyOnStop();
        }
    }

    protected void exitRecvBody() {

        Spliceable spliceable;

        payloadBuf.clear();
        if (payloadBuf.getInt(0) > INT_SIZE) {
            spliceable = spliceableFac.createSpliceable(payloadBuf);
            // track how may bytes have been received
            bytesReceived += payloadBuf.getInt(0);
            recordsReceived += 1;
            if (log.isDebugEnabled()) {
                log.debug("created a spliceable of length: " + payloadBuf.getInt(0));
            }

            pushSpliceable(spliceable);
        }
    }

    protected void enterSplicerWait() {
        // this is a place holder and is re implemented
        // in SpliceablePayloadReceiveChannel.
        // note that there is no exitSplicerWait() method.
        // we are just waiting until we are allowed to execute
        // the exitRecvBody() method.  See processTimer() code.
        //transition(SIG_DONE);
    }

    protected boolean handleMorePayloads()
    {
        if (splicerAvailable()) {
            transition(SIG_DONE);

            if (bufPos + INT_SIZE < inputBuf.position()) {
                return true;
            }
        }

        return false;
    }

    protected boolean splicerAvailable() {
        // placeholder for code in SpliceablePayloadReceiveChannel
        return true;
    }

    protected void notifyOnStop() {
        // since this is a SpliceablePayloadReceiveChannel, we
        // will have to shut down the splicer if necessary
        if (!isStopped) {
            Spliceable spliceable;
            spliceable = Splicer.LAST_POSSIBLE_SPLICEABLE;
            if (log.isInfoEnabled()) {
                log.info("pushing LAST_POSSIBLE_SPLICEABLE");
            }
            pushSpliceable(spliceable);
            if (!strandTail.isClosed()) {
                strandTail.close();
            }
            isStopped = true;
        }

        super.notifyOnStop();
    }

    private void pushSpliceable(Spliceable spliceable) {
        if (spliceable == null) {
            if (log.isErrorEnabled()) {
                log.error("Couldn't generate a payload from a buf: ");
                log.error("buf record: " + payloadBuf.getInt(0));
                log.error("buf limit: " + payloadBuf.limit());
                log.error("buf capacity: " + payloadBuf.capacity());
            }

            throw new RuntimeException("Couldn't create a Spliceable");
        }

        try {
            strandTail.push(spliceable);
        } catch (OrderingException oe) {
            // TODO: Need to be reviewed.
            if (log.isErrorEnabled()) {
                log.error("coudn't push a spliceable object: ", oe);
            }
        } catch (ClosedStrandException cse) {
            // TODO: Need to be reviewed.
            if (log.isErrorEnabled()) {
                log.error("coudn't push a spliceable object: ", cse);
            }
        }
    }

    public void setStrandTail(StrandTail strandTail) {
        if (strandTail == null) {
            throw new IllegalArgumentException("StrandTail cannot be null");
        }
        this.strandTail = strandTail;
    }
}
