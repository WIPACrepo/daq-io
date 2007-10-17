/*
 * class: PayloadReceiveChannel
 *
 * Version $Id: PayloadReceiveChannel.java 2125 2007-10-12 18:27:05Z ksb $
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
import icecube.daq.payload.VitreousBufferCache;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.NormalState;
import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.ErrorState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class provides the ReadableByteChannel operations. It is also responsible
 * for acquiring buffers into the buffer cache and managing the flow control.
 *
 * @author mcp
 * @version $Id: PayloadReceiveChannel.java 2125 2007-10-12 18:27:05Z ksb $
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
    protected ByteBuffer inputBuf;
    protected int bufPos;
    protected ByteBuffer payloadBuf;
    private int neededBufBlen;
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
                                 Semaphore inputSem)
    {
        id = myID;
        presState = STATE_IDLE;
        prevState = presState;
        selector = sel;
        cancelSelectorOnExit = false;
        bufferMgr = bufMgr;
        inputQueue = new LinkedQueue();
        inputAvailable = inputSem;
        inputBuf = ByteBuffer.allocate(2048);
        this.channel = channel;

        if (!(channel instanceof AbstractSelectableChannel)) {
            throw new Error("Cannot make channel non-blocking");
        }

        AbstractSelectableChannel selChan =
            (AbstractSelectableChannel) channel;
        if (selChan.isBlocking()) {
            try {
                selChan.configureBlocking(false);
            } catch (IOException ioe) {
                log.error("Couldn't configure channel to non-blocking mode",
                          ioe);
            }
        }

        setCacheLimits();
    }

    protected void setCacheLimits()
    {
        allocationStopped = false;
        if (bufferMgr.getIsCacheBounded()) {
            long maxAllocation =
                    bufferMgr.getMaxAquiredBytes();
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

    public void setInputBufferSize(int size)
    {
        if (inputBuf.position() != 0 || bufPos != 0) {
            throw new Error("Cannot set buffer size while buffer is in use");
        } else if (size <= 0) {
            throw new Error("Bad byte buffer size: " + size);
        }

        inputBuf = ByteBuffer.allocate(size);
    }

    /**
     * Return byte buffer to the buffer cache manager.
     *
     * @param buf byte buffer being returned
     */
    public void returnBuffer(ByteBuffer buf)
    {
        bufferMgr.returnBuffer(buf);
    }

    protected void exitIdle()
    {
        // compute some buffer manager limits...we do it here so
        // that we can pick up new values before starting the engine
        setCacheLimits();
    }

    protected void exitRecvHeader()
    {
        // do nothing
    }

    protected void exitRecvBody()
    { 
       if (payloadBuf.getInt(0) == INT_SIZE) {
            log.info("PayloadReceiveChannel " + id + " received STOP msg");
            // count stop message tokens
            stopMsgReceived += 1;
        } else {
            if (TRACE_DATA && log.isErrorEnabled()) {
                log.error(id + ":RECV:" +
                          icecube.daq.payload.DebugDumper.toString(payloadBuf));
            }
            // queue the output
            try {
                inputQueue.put(payloadBuf);
                if (TRACE_DATA && log.isErrorEnabled()) {
                    log.error(id + ":RECVBODY:Queued buffer is " +
                              (inputQueue.isEmpty() ? "" : "NOT ") + "empty");
                }
            } catch (InterruptedException e) {
                log.error("Could not enqueue buffer", e);
            }
            bytesReceived += payloadBuf.getInt(0);
            recordsReceived += 1;
            // let any waiters know data is available
            inputAvailable.release();
        }
    }

    protected void enterError()
    {
        if (compObserver != null) {
            compObserver.update(ErrorState.UNKNOWN_ERROR, notificationID);
        }
    }

    protected void enterSplicerWait()
    {
        // this is a place holder and is re implemented
        // in SpliceablePayloadReceiveChannel.
        // note that there is no exitSplicerWait() method.
        // we are just waiting until we are allowed to execute
        // the exitRecvBody() method.  See processTimer() code.
        transition(SIG_DONE);
    }

    protected boolean splicerAvailable()
    {
        // placeholder for code in SpliceablePayloadReceiveChannel
        return false;
    }

    protected void notifyOnStop()
    {
        if (compObserver != null) {
            compObserver.update(NormalState.STOPPED, notificationID);
        }
    }

    // all the following methods are mutex locked and thread safe.
    public void stopEngine()
    {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error("Couldn't stop engine", ie);
        }
        transition(SIG_FORCED_STOP);
        stateMachineMUTEX.release();
    }

    public void close()
        throws IOException
    {
        channel.close();
    }

    public void startEngine()
    {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error("Couldn't start engine", ie);
        }
        transition(SIG_START_HEADER);
        stateMachineMUTEX.release();
    }

    protected void startDisposing()
    {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error("Couldn't start disposing", ie);
        }
        transition(SIG_DISPOSE);
        stateMachineMUTEX.release();
    }

    public void injectError()
    {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException e) {
            log.error("Couldn't inject error", e);
        }
        transition(SIG_ERROR);
        stateMachineMUTEX.release();
    }

    public void registerComponentObserver(DAQComponentObserver compObserver,
                                          String notificationID)
    {
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

    public long getBufferCurrentAcquiredBytes()
    {
        return bufferMgr.getCurrentAquiredBytes();
    }

    public long getBufferCurrentAcquiredBuffers()
    {
        return bufferMgr.getCurrentAquiredBuffers();
    }

    public String presentState()
    {
        // don't need to interlock this one.
        return STATE_NAME_MAP[presState];
    }

    public void processTimer()
    {
        switch (presState) {
        case STATE_GETBUFFER:
            // we must have previously failed to get
            // a buffer...lets try again.
            // check for allocation limits--flow control

            if (allocationStopped) {
                if (bufferMgr.getCurrentAquiredBytes() <=
                    limitToRestartAllocation)
                {
                    // lets try to allocate
                    payloadBuf = bufferMgr.acquireBuffer(neededBufBlen);
                    // if successful, then reassert read interest
                    if (payloadBuf != null) {
                        allocationStopped = false;
                    }
                    // let things propagate through normally
                } else {
                    payloadBuf = null;
                }
            } else {
                // normal attempt to reallocate
                payloadBuf = bufferMgr.acquireBuffer(neededBufBlen);
            }
            if (payloadBuf != null) {
                if (!initializeBuffer()) {
                    transition(SIG_ERROR);
                } else {
                    // since we were successful, reassert read interest
                    try {
                        selectionKey =
                            ((SelectableChannel) channel).register(selector,
                                                                   SelectionKey.OP_READ,
                                                                   this);
                        // make sure we don't cance the selector when we exit
                        cancelSelectorOnExit = false;
                    } catch (ClosedChannelException cce) {
                        // flag error and return
                        log.warn("Exception during selector registration", cce);
                        transition(SIG_ERROR);
                        throw new RuntimeException(cce);
                    }
                }
                transition(SIG_START_BODY);
            }
            break;

        case STATE_RECVBODY:
            break;

        case STATE_DISPOSING:
            if ((System.currentTimeMillis() - startTimeMsec) >=
                DISPOSE_TIME_MSEC)
            {
                transition(SIG_IDLE);
            }
            break;

        case STATE_SPLICER_WAIT:
            // should not happen unless we are actually extended by
            // SpliceablePayloadReceiveChannel.
            if (splicerAvailable()) {
                transition(SIG_DONE);
            }
            break;

        default:
            break;
        }
    }

    protected boolean handleMorePayloads()
    {
        return (bufPos + INT_SIZE < inputBuf.position());
    }

    private void adjustOrExpandInputBuffer(int length)
    {
        inputBuf.limit(inputBuf.position());
        inputBuf.position(bufPos);
        bufPos = 0;

        if (length <= inputBuf.capacity()) {
            inputBuf.compact();
        } else {
            ByteBuffer newBuf = ByteBuffer.allocate(inputBuf.capacity() * 2);
            newBuf.put(inputBuf);
            inputBuf = newBuf;
        }
    }

    public void processSelect(SelectionKey selKey)
    {
        selectionKey = selKey;
        switch (presState) {
        case STATE_RECVHEADER:
            while (true) {
                if (inputBuf.position() < bufPos + INT_SIZE ||
                    inputBuf.position() < bufPos + inputBuf.getInt(bufPos))
                {
                    try {
                        int n = channel.read(inputBuf);
                        if (log.isDebugEnabled())
                            log.debug("STATE_RECVHEADER: Read " + n + " bytes - buffer position");
                    } catch (IOException ioe) {
                        //need to do something here
                        transition(SIG_ERROR);
                        log.error("Problem on channel.write(): ", ioe);
                        throw new RuntimeException(ioe);
                    }
                }

                if (inputBuf.position() < bufPos + 4) {
                    adjustOrExpandInputBuffer(4);
                } else {
                    int length = inputBuf.getInt(bufPos);
                    if (length == INT_SIZE) {
                        // count stop message tokens
                        stopMsgReceived += 1;
                        exitRecvHeader();
                        notifyOnStop();
                        presState = STATE_IDLE;
                        break;
                    }

                    if (length < INT_SIZE) {
                        // this really should not happen
                        transition(SIG_ERROR);
                        break;
                    }

                    if (length <= inputBuf.position() - bufPos) {
                        exitRecvHeader();
                        getBuffer();
                        if (handleMorePayloads()) {
                            log.debug("handling more payloads - bufPos = " + bufPos);
                            continue;
                        }
                        break;
                    }

                    if (length < inputBuf.capacity() - bufPos) {
                        // didn't read enough, wait for more input
                        break;
                    }

                    adjustOrExpandInputBuffer(length);
                }
            }
            break;

        case STATE_DISPOSING:
            try {
                payloadBuf.position(0);
                payloadBuf.limit(payloadBuf.capacity());
                channel.read(payloadBuf);
            } catch (IOException ioe) {
                //need to do something here
                transition(SIG_ERROR);
                log.error("Problem on channel.read(): ", ioe);
                throw new RuntimeException(ioe);
            }
            break;

        default:
            // should not be getting selects here
            selectionKey.cancel();
            break;
        }

        if (cancelSelectorOnExit) {
            // its ok to do it now.
            selectionKey.cancel();
        }
    }

    protected void transition(int signal)
    {
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
            if (signal == SIG_ERROR || signal == SIG_START_HEADER ||
                signal == SIG_DISPOSE)
            {
                exitIdle();
            }

            switch (signal) {
            case SIG_ERROR:
                doTransition(signal, STATE_ERROR);
                break;
            case SIG_START_HEADER:
                doTransition(signal, STATE_RECVHEADER);
                break;
            case SIG_DISPOSE:
                doTransition(signal, STATE_DISPOSING);
                break;
            default:
                break;
            }

            break;

        case STATE_GETBUFFER:
            switch (signal) {
            case SIG_ERROR:
                doTransition(signal, STATE_ERROR);
                break;
            case SIG_START_BODY:
                doTransition(signal, STATE_RECVBODY);
                break;
            case SIG_FORCED_STOP:
                doTransition(signal, STATE_IDLE);
                break;
            default:
                break;
            }

            break;

        case STATE_RECVHEADER:
            if (signal == SIG_ERROR || signal == SIG_GET_BUFFER ||
                signal == SIG_DONE || signal == SIG_FORCED_STOP)
            {
                exitRecvHeader();
            }

            switch (signal) {
            case SIG_ERROR:
                doTransition(signal, STATE_ERROR);
                break;
            case SIG_GET_BUFFER:
                doTransition(signal, STATE_GETBUFFER);
                break;
            case SIG_DONE:
                doTransition(signal, STATE_RECVDONE);
                break;
            case SIG_FORCED_STOP:
                doTransition(signal, STATE_IDLE);
                break;
            default:
                break;
            }

            break;

        case STATE_RECVBODY:
            if (signal == SIG_ERROR || signal == SIG_DONE ||
                signal == SIG_FORCED_STOP)
            {
                exitRecvBody();
            }

            switch (signal) {
            case SIG_ERROR:
                doTransition(signal, STATE_ERROR);
                break;
            case SIG_DONE:
                doTransition(signal, STATE_RECVDONE);
                break;
            case SIG_FORCED_STOP:
                doTransition(signal, STATE_IDLE);
                break;
            default:
                break;
            }

            break;

        case STATE_RECVDONE:
            switch (signal) {
            case SIG_ERROR:
                doTransition(signal, STATE_ERROR);
                break;
            case SIG_RESTART:
                doTransition(signal, STATE_RECVHEADER);
                break;
            case SIG_LAST_MSG:
                doTransition(signal, STATE_IDLE);
                break;
            case SIG_FORCED_STOP:
                doTransition(signal, STATE_IDLE);
                break;
            default:
                break;
            }

            break;

        case STATE_SPLICER_WAIT:
            if (signal == SIG_DONE) {
                // now we can execute exitRecvBody
                exitRecvBody();
            }

            switch (signal) {
            case SIG_ERROR:
                doTransition(signal, STATE_ERROR);
                break;
            case SIG_FORCED_STOP:
                doTransition(signal, STATE_IDLE);
                break;
            case SIG_DONE:
                doTransition(signal, STATE_RECVDONE);
                break;
            default:
                break;
            }

            break;

        case STATE_DISPOSING:
            if (signal == SIG_ERROR || signal == SIG_IDLE ||
                signal == SIG_FORCED_STOP)
            {
                selectionKey.cancel();
            }

            switch (signal) {
            case SIG_ERROR:
                doTransition(signal, STATE_ERROR);
                break;
            case SIG_IDLE:
                doTransition(signal, STATE_IDLE);
                break;
            case SIG_FORCED_STOP:
                doTransition(signal, STATE_IDLE);
                break;
            default:
                break;
            }

            break;

        case STATE_ERROR:
            if (signal == SIG_IDLE) {
                doTransition(signal, STATE_IDLE);
            }

            break;

        default:
            break;
        }
    }

    private boolean initializeBuffer()
    {
        if (payloadBuf.capacity() < neededBufBlen) {
            if (TRACE_DATA && log.isErrorEnabled()) {
                log.error(id + ":RECV:Buf is too small");
            }
            return false;
        }

        // now fix up actual receive buffer and copy in length
        payloadBuf.position(0);
        payloadBuf.limit(neededBufBlen);

        int pos = inputBuf.position();
        int lim = inputBuf.limit();

        if (lim != inputBuf.capacity()) {
            log.error("Surprise!  Input buffer " + inputBuf +
                      " capacity  != limit");
        }

        inputBuf.position(bufPos);
        inputBuf.limit(bufPos + neededBufBlen);

        payloadBuf.put(inputBuf);

        inputBuf.limit(lim);

        if (bufPos + neededBufBlen == pos) {
            inputBuf.position(0);
            bufPos = 0;
        } else {
            inputBuf.position(pos);
            bufPos += neededBufBlen;
        }

        return true;
    }

    private void getBuffer()
    {
        prevState = presState;
        presState = STATE_GETBUFFER;

        // received header, check for illegal length and allocate buffer
        neededBufBlen = inputBuf.getInt(bufPos);
        if (TRACE_DATA && log.isErrorEnabled()) {
            log.error(id + ":RECV:GetBuf LEN=" + neededBufBlen);
        }
        if (neededBufBlen < INT_SIZE) {
            transition(SIG_ERROR);
            return;
        }
        // check for allocation limits--flow control
        if (!allocationStopped) {
            if (bufferMgr.getCurrentAquiredBytes() >=
                limitToStopAllocation)
            {
                if (log.isErrorEnabled()) {
                    log.error(id + ":RECV:AcqBytes " +
                              bufferMgr.getCurrentAquiredBytes() + " >= " +
                              limitToStopAllocation);
                }
                allocationStopped = true;
                payloadBuf = null;
            } else {
                payloadBuf = bufferMgr.acquireBuffer(neededBufBlen);
            }
        }

        // verify correct capacity, if buffer was allocated
        if (payloadBuf == null) {
            if (TRACE_DATA && log.isErrorEnabled()) {
                log.error(id + ":RECV:Buf was null");
            }
            // if payloadBuf == null, always cancel selector interest
            selectionKey.cancel();
        } else if (!initializeBuffer()) {
            transition(SIG_ERROR);
        } else {
            presState = STATE_RECVBODY;
            if (splicerAvailable()) {
                // do not execute exitRecvBody because we
                // need to wait for permission to log to splicer
                presState = STATE_SPLICER_WAIT;
                enterSplicerWait();
            } else {
                exitRecvBody();
                presState = STATE_RECVDONE;
                // finish normally
                // note: inputBuf always contains the length of the msg.
                // if its a stop message, payloadBuf will not have been
                //  filled in, so we need to check inputBuf, not payloadBuf
                if (bufPos + INT_SIZE <= inputBuf.position() &&
                    inputBuf.getInt(bufPos) == INT_SIZE)
                {
                    notifyOnStop();
                    receivedLastMsg = false;
                } else {
                    restart();
                    presState = STATE_RECVHEADER;
                }
            }
        }
    }

    private void restart()
    {
        isStopped = false;
        try {
            selectionKey =
                ((SelectableChannel) channel).register(selector,
                                                       SelectionKey.OP_READ,
                                                       this);
            // make sure we don't cancel the selector when we exit
            cancelSelectorOnExit = false;
        } catch (ClosedChannelException cce) {
            // flag error and return
            log.error("Channel is closed: ", cce);
            transition(SIG_ERROR);
            throw new RuntimeException(cce);
        }
    }

    protected void doTransition(int signal, int nextState) {
        prevState = presState;
        presState = nextState;

        switch (nextState) {
        case STATE_DISPOSING:
            startTimeMsec = System.currentTimeMillis();
            try {
                selectionKey =
                    ((SelectableChannel) channel).register(selector,
                                                           SelectionKey.OP_READ,
                                                           this);
            } catch (ClosedChannelException cce) {
                // flag error and return
                log.error("Selector register error during disposing", cce);
                transition(SIG_ERROR);
                throw new RuntimeException(cce);
            }
            break;
        case STATE_ERROR:
            enterError();
            break;
        case STATE_GETBUFFER:
            getBuffer();
            break;
        case STATE_IDLE:
            if (signal == SIG_FORCED_STOP || signal == SIG_LAST_MSG) {
                notifyOnStop();
            }
            receivedLastMsg = false;
            break;
        case STATE_RECVBODY:
            break;
        case STATE_RECVDONE:
            // note: inputBuf always contains the length of the msg.
            // if its a stop message, buf will not have been filled in,
            // so we need to check inputBuf, not buf
            if (inputBuf.getInt(bufPos) == INT_SIZE) {
                transition(SIG_LAST_MSG);
            } else {
                transition(SIG_RESTART);
            }
            break;
        case STATE_RECVHEADER:
            restart();
            break;
        case STATE_SPLICER_WAIT:
            enterSplicerWait();
            break;
        default:
            break;
        }
    }

    private static final String getStateName(int state)
    {
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

    private static final String getSignalName(int signal)
    {
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

    private void logIllegalTransition(int signal)
    {
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

    public String toString()
    {
        return "PayloadReceiveChannel#" + num + "[" + channel + "]";
    }

    private static int nextNum = 0;
    private int num = nextNum++;

    public int getNum()
    {
        return num;
    }
}
