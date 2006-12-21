/*
 * class: PayloadReceiveChannel
 *
 * Version $Id: PayloadReceiveChannel.java,v 1.24 2005/12/20 00:51:14 mcp Exp $
 *
 * Date: May 15 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.Mutex;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.Semaphore;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ByteBufferCache;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.NormalState;
import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.ErrorState;

import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class provides the ReadableByteChannel operations. It is also responsible
 * for acquiring buffers into the buffer cache and managing the flow control.
 *
 * @author mcp
 * @version $Id: PayloadReceiveChannel.java,v 1.24 2005/12/20 00:51:14 mcp Exp $
 */
public class PayloadReceiveChannel {

    protected static final int STATE_IDLE = 0;
    protected static final int STATE_GETBUFFER = 1;
    protected static final int STATE_RECVHEADER = 2;
    protected static final int STATE_RECVBODY = 3;
    protected static final int STATE_RECVDONE = 4;
    protected static final int STATE_DISPOSING = 5;
    protected static final int STATE_ERROR = 6;
    protected static final int STATE_SPLICER_WAIT = 7;

    protected static final int SIG_IDLE = 0;
    protected static final int SIG_GET_BUFFER = 1;
    protected static final int SIG_START_HEADER = 2;
    protected static final int SIG_START_BODY = 3;
    protected static final int SIG_DONE = 4;
    protected static final int SIG_RESTART = 5;
    protected static final int SIG_DISPOSE = 6;
    protected static final int SIG_ERROR = 7;
    protected static final int SIG_FORCED_STOP = 8;
    protected static final int SIG_LAST_MSG = 9;
    protected static final int SIG_WAIT_FOR_SPLICER = 10;

    public static final String STATE_IDLE_NAME = "Idle";
    public static final String STATE_GETBUFFER_NAME = "GetBuffer";
    public static final String STATE_RECVHEADER_NAME = "RecvHeader";
    public static final String STATE_RECVBODY_NAME = "RecvBody";
    public static final String STATE_RECVDONE_NAME = "RecvDone";
    public static final String STATE_DISPOSING_NAME = "Disposing";
    public static final String STATE_ERROR_NAME = "Error";
    public static final String STATE_SPLICER_WAIT_NAME = "WaitForSplicer";

    private static final String[] STATE_NAME_MAP = {STATE_IDLE_NAME,
                                                    STATE_GETBUFFER_NAME,
                                                    STATE_RECVHEADER_NAME,
                                                    STATE_RECVBODY_NAME,
                                                    STATE_RECVDONE_NAME,
                                                    STATE_DISPOSING_NAME,
                                                    STATE_ERROR_NAME,
                                                    STATE_SPLICER_WAIT_NAME};

    private static final long DISPOSE_TIME_MSEC = 500;
    private static final int BYTE_COUNT_LEN = 4;
    protected static final int INT_SIZE = 4;
    private static final long DEFAULT_PERCENT_STOP_ALLOCATION = 70;
    private static final long DEFAULT_PERCENT_RESTART_ALLOCATION = 50;
    private static final long DEFAULT_MAX_BYTES_ALLOCATION_LIMIT = 20000000;

    // print log messages with state change information?
    private static final boolean TRACE_STATE = false;
    // print log messages with data
    private static final boolean TRACE_DATA = false;

    // private instance member data
    // sempahore to be used as a mutex to lock out cuncurrent ops
    // between wait code and channel locator callbacks
    private Mutex stateMachineMUTEX = new Mutex();
    // our internal state
    private int presState;
    // our last state
    private int prevState;
    // count transitions
    private int transitionCnt = 0;
    // count illeagal transition requests
    private int illegalTransitionCnt = 0;
    // data channel mbean
    private ReadableByteChannel channel = null;
    // local copy of selector
    private Selector selector;
    // local copy of selector key
    private SelectionKey selectionKey = null;

    // flag for end of data notification
    private boolean receivedLastMsg;
    // counters for timer implemetation
    private long startTimeMsec;
    // buffer cache manager that is source of receive buffers
    private IByteBufferCache bufferMgr;
    // receive buffer in use
    protected ByteBuffer headerBuf;
    protected ByteBuffer buf;
    protected int neededBufBlen;
    // output buffer queue
    public LinkedQueue inputQueue;
    // my ID used for callback identification
    protected String id;
    // local boolean to synchronize selector cancels and
    // prevent cancelled selector key exceptions
    private boolean cancelSelectorOnExit;
    // semaphore to indicate some completed receive activity
    protected Semaphore inputAvailable;
    // local transmit counters
    protected long bytesReceived = 0;
    protected long recordsReceived = 0;
    protected long stopMsgReceived = 0;
    // set up logging channel for this component
    private Log log = LogFactory.getLog(PayloadReceiveChannel.class);
    // buffer manager limits....receive engines only
    protected long limitToStopAllocation = 0;
    protected long limitToRestartAllocation = 0;
    protected boolean allocationStopped = false;
    protected long percentOfMaxStopAllocation = DEFAULT_PERCENT_STOP_ALLOCATION;
    protected long percentOfMaxRestartAllocation = DEFAULT_PERCENT_RESTART_ALLOCATION;
    protected boolean isStopped = true;

    protected DAQComponentObserver compObserver;
    protected String notificationID;

    private boolean debug = false;

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public PayloadReceiveChannel(String myID,
                                 Selector sel,
                                 ReadableByteChannel channel,
                                 IByteBufferCache bufMgr,
                                 Semaphore inputSem) {
        id = myID;
        presState = STATE_IDLE;
        prevState = presState;
        selector = sel;
        cancelSelectorOnExit = false;
        bufferMgr = bufMgr;
        inputQueue = new LinkedQueue();
        inputAvailable = inputSem;
        headerBuf = ByteBuffer.allocate(INT_SIZE);
        this.channel = channel;

        setCacheLimits();
    }

    protected void setCacheLimits() {
        allocationStopped = false;
        if (((ByteBufferCache) bufferMgr).getIsCacheBounded()) {
            long maxAllocation =
                    ((ByteBufferCache) bufferMgr).getMaxAquiredBytes();
            limitToStopAllocation = (maxAllocation *
                    percentOfMaxStopAllocation) / 100;
            limitToRestartAllocation = (maxAllocation *
                    percentOfMaxRestartAllocation) / 100;
        } else {
            limitToStopAllocation = (DEFAULT_MAX_BYTES_ALLOCATION_LIMIT *
                    percentOfMaxStopAllocation) / 100;
            limitToRestartAllocation = (DEFAULT_MAX_BYTES_ALLOCATION_LIMIT *
                    percentOfMaxRestartAllocation) / 100;
        }
    }

    // instance member method (alphabetic)
    protected void enterIdle() {
        receivedLastMsg = false;
    }

    protected void exitIdle() {
        // compute some buffer manager limits...we do it here so
        // that we can pick up new values before starting the engine
        setCacheLimits();
    }

    protected void enterGetBuffer() {
        // received header, check for illegal length and allocate buffer
        neededBufBlen = headerBuf.getInt(0);
        if (TRACE_DATA && log.isErrorEnabled()) {
            log.error(id + ":RECV:GetBuf LEN=" + neededBufBlen);
        }
        if (neededBufBlen < INT_SIZE) {
            transition(SIG_ERROR);
            return;
        }
        // check for allocation limits--flow control
        if (!allocationStopped) {
            if (bufferMgr.getCurrentAquiredBytes()
                    >= limitToStopAllocation) {
                if (TRACE_DATA && log.isErrorEnabled()) {
                    log.error(id + ":RECV:AcqBytes " +
                            bufferMgr.getCurrentAquiredBytes() + " >= " +
                            limitToStopAllocation);
                }
                allocationStopped = true;
                //System.out.println("allocationStopped");
                buf = null;
            } else {
                buf = bufferMgr.acquireBuffer(neededBufBlen);
            }
        }

        // verify correct capacity, if buffer was allocated
        if (buf != null) {
            if (buf.capacity() < neededBufBlen) {
                if (TRACE_DATA && log.isErrorEnabled()) {
                    log.error(id + ":RECV:Buf is too small");
                }
                transition(SIG_ERROR);
            } else {
                // now fix up actual receive buffer and copy in length
                buf.position(0);
                buf.limit(neededBufBlen);
                buf.putInt(neededBufBlen);
                transition(SIG_START_BODY);
            }
        } else {
            if (TRACE_DATA && log.isErrorEnabled()) {
                log.error(id + ":RECV:Buf was null");
            }
            // if buf == null, always cancel selector interest
            selectionKey.cancel();
        }
    }

    protected void exitGetBuffer() {
    }

    protected void enterRecvHeader() {
        isStopped = false;
        headerBuf.clear();
        try {
            selectionKey = ((SelectableChannel) channel).register(selector,
                    SelectionKey.OP_READ, this);
            // make sure we don't cancel the selector when we exit
            cancelSelectorOnExit = false;
        } catch (ClosedChannelException cce) {
            // flag error and return
            log.error("Channel is closed: ", cce);
            transition(SIG_ERROR);
            throw new RuntimeException(cce);
        }
    }

    protected void exitRecvHeader() {

    }

    protected void enterRecvBody() {
        buf.position(0);
        int length = buf.getInt();
        if (buf.capacity() < length) {
            log.warn("buffer capacity (" + buf.capacity() +
                    ") less than message length (" + length);
            transition(SIG_ERROR);
        }
        buf.limit(length);
        if (TRACE_DATA && log.isErrorEnabled()) {
            log.error(id + ":RECVBODY:Buflen=" + length);
        }
    }

    protected void exitRecvBody() {
        if (buf.getInt(0) != INT_SIZE) {
            if (TRACE_DATA && log.isErrorEnabled()) {
                log.error(id + ":RECV:" + icecube.daq.payload.DebugDumper.toString(buf));
            }
            // queue the output
            try {
                inputQueue.put(buf);
                if (TRACE_DATA && log.isErrorEnabled()) {
                    log.error(id + ":RECVBODY:Queued buffer is " + (inputQueue.isEmpty() ? "" : "NOT ") + "empty");
                }
            } catch (InterruptedException e) {
                log.error("Could not enqueue buffer", e);
            }
            bytesReceived += buf.getInt(0);
            recordsReceived += 1;
            // let any waiters know data is available
            inputAvailable.release();
        } else {
            log.info("PayloadReceiveChannel " + id + " received STOP msg");
            // count stop message tokens
            stopMsgReceived += 1;
        }
    }

    protected void enterRecvDone() {
        // note: headerBuf always contains the length of the msg.
        // if its a stop message, buf will not have been filled in,
        // so we need to check headerBuf, not buf
        if (headerBuf.getInt(0) == INT_SIZE) {
            transition(SIG_LAST_MSG);
        } else {
            transition(SIG_RESTART);
        }
    }

    protected void exitRecvDone() {
    }

    protected void enterDisposing() {
        startTimeMsec = System.currentTimeMillis();
        try {
            selectionKey = ((SelectableChannel) channel).register(selector,
                    SelectionKey.OP_READ, this);
        } catch (ClosedChannelException cce) {
            // flag error and return
            log.error("Selector register error during disposing: " + cce);
            transition(SIG_ERROR);
            throw new RuntimeException(cce);
        }
    }

    protected void exitDisposing() {
        selectionKey.cancel();
    }

    protected void enterError() {
        if (compObserver != null) {
            compObserver.update(ErrorState.UNKNOWN_ERROR, notificationID);
        }
    }

    protected void exitError() {
    }

    protected void enterSplicerWait() {
        // this is a place holder and is re implemented
        // in SpliceablePayloadReceiveChannel.
        // note that there is no exitSplicerWait() method.
        // we are just waiting until we are allowed to execute
        // the exitRecvBody() method.  See processTimer() code.
        transition(SIG_DONE);
    }

    protected boolean splicerAvailable() {
        // placeholder for code in SpliceablePayloadReceiveChannel
        return true;
    }

    protected void notifyOnStop() {
        if (compObserver != null) {
            compObserver.update(NormalState.STOPPED, notificationID);
        }
    }

    // all the following methods are mutex locked and thread safe.
    public void stopEngine() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        transition(SIG_FORCED_STOP);
        stateMachineMUTEX.release();
    }

    public void startEngine() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        transition(SIG_START_HEADER);
        stateMachineMUTEX.release();
    }

    protected void startDisposing() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        transition(SIG_DISPOSE);
        stateMachineMUTEX.release();
    }

    public void injectError() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException e) {
            log.error(e);
        }
        transition(SIG_ERROR);
        stateMachineMUTEX.release();
    }

    public void registerComponentObserver(DAQComponentObserver compObserver,
                                          String notificationID) {
        if (this.compObserver != null) {
            try {
                throw new Error("StackTrace");
            } catch (Error err) {
                log.error("Setting multiple observers: OLD (" +
                          this.compObserver + ":" + this.notificationID +
                          ") NEW (" + compObserver + ":" + notificationID +
                          ")", err);
            }
        }

        this.compObserver = compObserver;
        this.notificationID = notificationID;
    }

    public long getBufferCurrentAcquiredBytes() {
        return ((ByteBufferCache) bufferMgr).
                getCurrentAquiredBytes();
    }

    public long getBufferCurrentAcquiredBuffers() {
        return ((ByteBufferCache) bufferMgr).
                getCurrentAquiredBuffers();
    }

    public String presentState() {
        // don't need to interlock this one.
        return STATE_NAME_MAP[presState];
    }

    public void processTimer() {
        switch (presState) {
            case STATE_GETBUFFER:
                {
                    // we must have previously failed to get
                    // a buffer...lets try again.
                    // check for allocation limits--flow control

                    if (allocationStopped) {
                        if (((ByteBufferCache) bufferMgr).getCurrentAquiredBytes()
                                <= limitToRestartAllocation) {
                            // lets try to allocate
                            buf = bufferMgr.acquireBuffer(neededBufBlen);
                            // if successful, then reassert read interest
                            if (buf != null) {
                                allocationStopped = false;
                            }
                            // let things propagate through normally
                        } else {
                            buf = null;
                        }
                    } else {
                        // normal attempt to reallocate
                        buf = bufferMgr.acquireBuffer(neededBufBlen);
                    }
                    if (buf != null) {
                        if (buf.capacity() < BYTE_COUNT_LEN) {
                            transition(SIG_ERROR);
                        } else {
                            // lets make sure that the buffer is properly initialized
                            buf.position(0);
                            buf.limit(neededBufBlen);
                            buf.putInt(neededBufBlen);
                            // since we were successful, reassert read interest
                            try {
                                selectionKey = ((SelectableChannel) channel).register(selector,
                                        SelectionKey.OP_READ, this);
                                // make sure we don't cance the selector when we exit
                                cancelSelectorOnExit = false;
                            } catch (ClosedChannelException cce) {
                                // flag error and return
                                log.warn("Exception during selector registration: " + cce);
                                transition(SIG_ERROR);
                                throw new RuntimeException(cce);
                            }
                            transition(SIG_START_BODY);
                        }
                    }
                    break;
                }
            case STATE_DISPOSING:
                {
                    if ((System.currentTimeMillis() - startTimeMsec) >=
                            DISPOSE_TIME_MSEC) {
                        transition(SIG_IDLE);
                    }
                    break;
                }
            case STATE_SPLICER_WAIT:
                {
                    // should not happen unless we are actually extended by
                    // SpliceablePayloadReceiveChannel.
                    if (this.splicerAvailable()) {
                        transition(SIG_DONE);
                    }
                    break;
                }
            default:
                {
                    break;
                }
        }
    }

    public void processSelect(SelectionKey selKey) {
        selectionKey = selKey;
        switch (presState) {
            case (STATE_RECVHEADER):
                {
                    try {
                        channel.read(headerBuf);
                    } catch (IOException ioe) {
                        //need to do something here
                        transition(SIG_ERROR);
                        log.error("Problem on channel.write(): ", ioe);
                        throw new RuntimeException(ioe);
                    }
                    if (!headerBuf.hasRemaining()) {
                        int length = headerBuf.getInt(0);
                        if (length == INT_SIZE) {
                            // count stop message tokens
                            stopMsgReceived += 1;
                            transition(SIG_DONE);
                        } else if (length < INT_SIZE) {
                            // this really should not happen
                            transition(SIG_ERROR);
                        } else {
                            transition(SIG_GET_BUFFER);
                        }
                    }
                    break;
                }
            case (STATE_RECVBODY):
                {
                    try {
                        channel.read(buf);
                    } catch (IOException ioe) {
                        //need to do something here
                        transition(SIG_ERROR);
                        log.error("Problem on channel.read(): ", ioe);
                        throw new RuntimeException(ioe);
                    }
                    if (!buf.hasRemaining()) {
                        cancelSelectorOnExit = true;
                        if (splicerAvailable()) {
                            // finish normally
                            transition(SIG_DONE);
                        } else {
                            // pend until splicer is available
                            transition(SIG_WAIT_FOR_SPLICER);
                        }
                    }
                    break;
                }
            case (STATE_DISPOSING):
                {
                    try {
                        buf.position(0);
                        buf.limit(buf.capacity());
                        channel.read(buf);
                    } catch (IOException ioe) {
                        //need to do something here
                        transition(SIG_ERROR);
                        log.error("Problem on channel.read(): ", ioe);
                        throw new RuntimeException(ioe);
                    }
                    break;
                }
            default:
                {
                    // should not be getting selects here
                    selectionKey.cancel();
                    break;
                }
        }
        if (cancelSelectorOnExit) {
            // its ok to do it now.
            selectionKey.cancel();
        }
    }

    protected void transition(int signal) {
        if (log.isDebugEnabled() && TRACE_STATE) {
            log.debug("PayloadReceiveChannel " + id +
                    " state " + getStateName(presState) +
                    " transition signal " + getSignalName(signal));
        }
        if (debug) System.err.println(id + " " + getStateName(presState) + " -> " + getSignalName(signal));
        // note, in order to simplify state machine operation, NO
        // transitions are allowed in state exit routines.  They are allowed
        // in state enter routines, since the present state flag
        // has been appropriately changed and things are reentrant
        switch (presState) {
            case STATE_IDLE:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitIdle();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_START_HEADER:
                            {
                                exitIdle();
                                doTransition(STATE_RECVHEADER);
                                enterRecvHeader();
                                break;
                            }
                        case SIG_DISPOSE:
                            {
                                exitIdle();
                                doTransition(STATE_DISPOSING);
                                enterDisposing();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_GETBUFFER:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitGetBuffer();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_START_BODY:
                            {
                                exitGetBuffer();
                                doTransition(STATE_RECVBODY);
                                enterRecvBody();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitGetBuffer();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_RECVHEADER:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitRecvHeader();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_GET_BUFFER:
                            {
                                exitRecvHeader();
                                doTransition(STATE_GETBUFFER);
                                enterGetBuffer();
                                break;
                            }
                        case SIG_DONE:
                            {
                                exitRecvHeader();
                                doTransition(STATE_RECVDONE);
                                enterRecvDone();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitRecvHeader();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_RECVBODY:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitRecvBody();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_DONE:
                            {
                                exitRecvBody();
                                doTransition(STATE_RECVDONE);
                                enterRecvDone();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitRecvBody();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_WAIT_FOR_SPLICER:
                            {
                                // do not execute exitRecvBody because we
                                // need to wait for permission to log to splicer
                                doTransition(STATE_SPLICER_WAIT);
                                enterSplicerWait();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_RECVDONE:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitRecvDone();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_RESTART:
                            {
                                exitRecvDone();
                                doTransition(STATE_RECVHEADER);
                                enterRecvHeader();
                                break;
                            }
                        case SIG_LAST_MSG:
                            {
                                exitRecvDone();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitRecvDone();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_SPLICER_WAIT:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitRecvDone();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitRecvDone();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_DONE:
                            {
                                // now we can execute exitRecvBody
                                exitRecvBody();
                                doTransition(STATE_RECVDONE);
                                enterRecvDone();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_DISPOSING:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitDisposing();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_IDLE:
                            {
                                exitDisposing();
                                doTransition(STATE_IDLE);
                                enterIdle();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitDisposing();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_ERROR:
                {
                    switch (signal) {
                        case SIG_IDLE:
                            {
                                exitError();
                                doTransition(STATE_IDLE);
                                enterIdle();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            default:
                {
                    illegalTransitionCnt++;
                    break;
                }
        }
    }

    private void doTransition(int nextState) {
        prevState = presState;
        presState = nextState;
        transitionCnt++;
    }

    private static final String getStateName(int state) {
        final String name;
        switch (state) {
            case STATE_IDLE:
                name = STATE_IDLE_NAME;
                break;
            case STATE_GETBUFFER:
                name = STATE_GETBUFFER_NAME;
                break;
            case STATE_RECVHEADER:
                name = STATE_RECVHEADER_NAME;
                break;
            case STATE_RECVBODY:
                name = STATE_RECVBODY_NAME;
                break;
            case STATE_RECVDONE:
                name = STATE_RECVDONE_NAME;
                break;
            case STATE_DISPOSING:
                name = STATE_DISPOSING_NAME;
                break;
            case STATE_ERROR:
                name = STATE_ERROR_NAME;
                break;
            case STATE_SPLICER_WAIT:
                name = STATE_SPLICER_WAIT_NAME;
                break;
            default:
                name = "UNKNOWN#" + state;
                break;
        }
        return name;
    }

    private static final String getSignalName(int signal) {
        final String name;
        switch (signal) {
            case SIG_IDLE:
                name = "IDLE";
                break;
            case SIG_GET_BUFFER:
                name = "GET_BUFFER";
                break;
            case SIG_START_HEADER:
                name = "START_HEADER";
                break;
            case SIG_START_BODY:
                name = "START_BODY";
                break;
            case SIG_DONE:
                name = "DONE";
                break;
            case SIG_RESTART:
                name = "RESTART";
                break;
            case SIG_DISPOSE:
                name = "DISPOSE";
                break;
            case SIG_ERROR:
                name = "ERROR";
                break;
            case SIG_FORCED_STOP:
                name = "FORCED_STOP";
                break;
            case SIG_LAST_MSG:
                name = "LAST_MSG";
                break;
            case SIG_WAIT_FOR_SPLICER:
                name = "WAIT_FOR_SPLICER";
                break;
            default:
                name = "UNKNOWN#" + signal;
                break;
        }
        return name;
    }

    private void logIllegalTransition(int signal) {
/*
        if (log.isInfoEnabled()) {
            final String errMsg =
                "PayloadReceiveChannel " + id +
                " illegal transition for state " + getStateName(presState) +
                " signal " + getSignalName(signal);

            if (!TRACE_STATE) {
                log.info(errMsg);
            } else {
                try {
                    throw new IllegalStateException("Bad state");
                } catch (Exception ex) {
                    log.info(errMsg, ex);
                }
            }
        }
*/
    }

    public String toString() {
        return "PayloadReceiveChannel#" + num + "[" + channel + "]";
    }

    private static int nextNum = 0;
    private int num = nextNum++;

    public int getNum() {
        return num;
    }
}
