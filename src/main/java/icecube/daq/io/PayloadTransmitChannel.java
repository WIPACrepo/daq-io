/*
 * class: PayloadTransmitChannel
 *
 * Version $Id: PayloadTransmitChannel.java 17420 2019-06-25 23:14:19Z dglo $
 *
 * Date: May 15 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.Mutex;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

/**
 * This class provides the WritableByteChannel operations. It is also responsible
 * for returning buffers into the buffer cache.
 *
 * @author mcp
 * @version $Id: PayloadTransmitChannel.java 17420 2019-06-25 23:14:19Z dglo $
 */
public class PayloadTransmitChannel implements QueuedOutputChannel {

    // Log object for this class
    private static final Logger log = Logger.getLogger(PayloadTransmitChannel.class);

    // decalre some state transitions
    private static final int STATE_IDLE = 0;
    private static final int STATE_GETBUFFER = 1;
    private static final int STATE_TRANSMSG = 2;
    private static final int STATE_TRANSDONE = 3;
    private static final int STATE_ERROR = 4;
    private static final int STATE_CLOSED = 5;

    private static final int SIG_IDLE = 0;
    private static final int SIG_GET_BUFFER = 1;
    private static final int SIG_TRANSMIT = 2;
    private static final int SIG_DONE = 3;
    private static final int SIG_RESTART = 4;
    private static final int SIG_STOP = 5;
    private static final int SIG_ERROR = 6;
    private static final int SIG_CLOSE = 7;

    public static final String STATE_IDLE_NAME = "Idle";
    public static final String STATE_GETBUFFER_NAME = "GetBuffer";
    public static final String STATE_TRANSMSG_NAME = "TransMsg";
    public static final String STATE_TRANSDONE_NAME = "TransDone";
    public static final String STATE_ERROR_NAME = "Error";
    public static final String STATE_CLOSED_NAME = "Closed";

    private static final String[] STATE_NAME_MAP = {STATE_IDLE_NAME,
                                                    STATE_GETBUFFER_NAME,
                                                    STATE_TRANSMSG_NAME,
                                                    STATE_TRANSDONE_NAME,
                                                    STATE_ERROR_NAME,
                                                    STATE_CLOSED_NAME};

    private static final int INT_SIZE = 4;

    // print log messages with state change information?
    private static final boolean TRACE_STATE = false;

    // sempahore to be used as a mutex to lock out cuncurrent ops
    // between wait code and channel locator callbacks
    private Mutex stateMachineMUTEX = new Mutex();
    // our internal state
    private int presState;

    private WritableByteChannel channel = null;
    // local copy of selector
    private Selector selector;
    // flag for end of data notification
    private boolean lastMsgAndStop;
    // buffer cache manager that is source of receive buffers
    private IByteBufferCache bufferMgr;
    // receive buffer in use
    private ByteBuffer buf;
    // ID string used to identify us during callbacks
    private String id;
    // local transmit counters
    protected long bytesSent;
    protected long recordsSent;
    protected long stopMsgSent;
    // local boolean to synchronize selector cancels and
    // prevent cancelled selector key exceptions
    private boolean cancelSelectorOnExit;
    private ByteBuffer lastMessage;

    // output buffer queue
    private DepthQueue outputQueue;
    private boolean queueStillEmpty;

    private DAQComponentObserver compObserver;
    private String notificationID;

    private boolean debug = false;

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public PayloadTransmitChannel(String myID,
                                  WritableByteChannel channel,
                                  Selector sel,
                                  IByteBufferCache bufMgr) {
        id = myID;
        presState = STATE_IDLE;
        cancelSelectorOnExit = false;
        selector = sel;
        bufferMgr = bufMgr;
        outputQueue = new DepthQueue();
        // make up a last message that we can use when we need it
        lastMessage = ByteBuffer.allocate(INT_SIZE);
        lastMessage.putInt(0, INT_SIZE);

        if (((SelectableChannel)channel).isBlocking()) {
            throw new Error("Channel " + channel + " is blocking");
        }

        this.channel = channel;
    }

    protected void enterIdle() {
    }

    protected void exitIdle() {
        lastMsgAndStop = false;
    }

    protected void enterGetBuffer() {
        if (outputQueue.isEmpty()) {
            if (debug && !queueStillEmpty) {
                if (log.isInfoEnabled()) {
                    log.info(id + ":XMIT:EmptyQueue");
                }
                queueStillEmpty = true;
            }
        } else {
            try {
                buf = (ByteBuffer) outputQueue.take();
                transition(SIG_TRANSMIT);
            } catch (InterruptedException e) {
                log.error(e);
            }
            if (debug) {
                if (log.isInfoEnabled()) {
                    log.info(id + ":XMIT:GotBuf " + buf);
                }
                queueStillEmpty = false;
            }
        }
    }

    protected void exitGetBuffer() {
    }

    protected void enterTransMsg() {
        try {
            ((SelectableChannel) channel).register(selector,
                    SelectionKey.OP_WRITE, this);
            // make sure we don't cance the selector when we exit
            cancelSelectorOnExit = false;
        } catch (IOException e) {
            log.error(id + " cannot transmit", e);
            // flag error and return
            transition(SIG_ERROR);
            throw new RuntimeException(e);
        }
        buf.position(0);
        buf.limit(buf.getInt(0));
        if (buf.getInt(0) == INT_SIZE) {
            // this is the last message, flag state machine to stop when complete
            lastMsgAndStop = true;
            if (debug) {
                if (log.isInfoEnabled()) {
                    log.info(id + ":XMIT:stop");
                }
            }
        }
        try {
            channel.write(buf);
            if (debug && log.isInfoEnabled()) {
                log.info(id + ":XMIT:wrote " + buf.getInt(0) + " bytes");
            }
        } catch (IOException e) {
            log.error("enterTransMsg error", e);
            //need to do something here
            transition(SIG_ERROR);
        }
    }

    protected void exitTransMsg() {
        // track some statistics
        bytesSent += buf.getInt(0);
        if (debug && log.isInfoEnabled()) {
            log.info(id + ":XMIT:sent " + bytesSent + " bytes");
        }
        recordsSent += 1;
        if (debug && log.isInfoEnabled()) {
            log.info(id + ":XMIT:sent " + recordsSent + " recs");
        }
        if (buf.getInt(0) == INT_SIZE) {
            stopMsgSent += 1;
        }
        // return the buffer via the supplied manager
        else {
            if (bufferMgr != null) {
                bufferMgr.returnBuffer(buf);
            }
        }
        cancelSelectorOnExit = true;
    }

    protected void enterTransDone() {
        if (lastMsgAndStop) {
            transition(SIG_STOP);
        } else {
            transition(SIG_RESTART);
        }
    }

    protected void exitTransDone() {
    }

    protected void enterError() {
        if (compObserver != null) {
            if (debug && log.isInfoEnabled()) {
                log.info(id + ":XMIT:Error");
            }
            compObserver.update(ErrorState.UNKNOWN_ERROR, notificationID);
        }
    }

    protected void exitError() {
    }

    protected void notifyOnStop() {
        if (compObserver != null) {
            if (debug && log.isInfoEnabled()) {
                log.info(id + ":XMIT:Stop");
            }
            compObserver.update(NormalState.STOPPED, notificationID);
        }
    }

    // all the following methods are mutex locked and thread safe.
    public void stopEngine() {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_STOP);
            stateMachineMUTEX.release();
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void sendLastAndStop() {
        try {
            outputQueue.put(lastMessage);
        } catch (InterruptedException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    public void startEngine() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException(ie);
        }
        transition(SIG_GET_BUFFER);
        stateMachineMUTEX.release();
    }

    @Override
    public void flushOutQueue() {
        selector.wakeup();
    }

    @Override
    public void registerComponentObserver(DAQComponentObserver compObserver,
                                          String notificationID) {
        if (this.compObserver != null) {
            try {
                throw new Error("StackTrace");
            } catch (Error err) {
                log.error("Setting multiple observers", err);
            }
        }

        this.compObserver = compObserver;
        this.notificationID = notificationID;
    }

    public String presentState() {
        // don't need to interlock this one.
        return STATE_NAME_MAP[presState];
    }

    public void injectError() {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_ERROR);
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException(ie);
        }
    }

    public void processTimer() {
        if (presState == STATE_GETBUFFER) {
            enterGetBuffer();
        }
    }

    public void processSelect(SelectionKey selKey) {
        if (presState != STATE_TRANSMSG) {
            // should not be getting selects here
            selKey.cancel();
        } else {
            if (!buf.hasRemaining()) {
                transition(SIG_DONE);
            } else {
                try {
                    channel.write(buf);
                } catch (IOException ioe) {
                    transition(SIG_ERROR);
                    log.error("IOException on channel.write(): ", ioe);
                    throw new RuntimeException(ioe);
                }
            }
            if (cancelSelectorOnExit) {
                // its ok to do it now.
                selKey.cancel();
            }
        }
    }

    void close()
        throws IOException
    {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_CLOSE);
            stateMachineMUTEX.release();
        } catch (InterruptedException e) {
            // should not happen
        }
    }

    private void enterClosed()
    {
        try {
            channel.close();
        } catch (IOException ioe) {
            log.error("Cannot close channel", ioe);
        }

        channel = null;
    }

    private void transition(int signal) {
        if (log.isDebugEnabled() && TRACE_STATE) {
            log.debug("PayloadTransmitEngine " + id +
                    " state " + getStateName(presState) +
                    " transition signal " + getSignalName(signal));
        } else if (debug && log.isInfoEnabled()) {
            log.info(id + " " + getStateName(presState) + " -> " +
                     getSignalName(signal));
        }

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
                        case SIG_GET_BUFFER:
                            {
                                exitIdle();
                                doTransition(STATE_GETBUFFER);
                                enterGetBuffer();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitIdle();
                                doTransition(STATE_CLOSED);
                                enterClosed();
                                break;
                            }
                        default:
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
                        case SIG_TRANSMIT:
                            {
                                exitGetBuffer();
                                doTransition(STATE_TRANSMSG);
                                enterTransMsg();
                                break;
                            }
                        case SIG_STOP:
                            {
                                exitGetBuffer();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitGetBuffer();
                                doTransition(STATE_CLOSED);
                                notifyOnStop();
                                enterClosed();
                                break;
                            }
                        default:
                            break;
                    }
                    break;
                }
            case STATE_TRANSMSG:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitTransMsg();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_DONE:
                            {
                                exitTransMsg();
                                doTransition(STATE_TRANSDONE);
                                enterTransDone();
                                break;
                            }
                        case SIG_STOP:
                            {
                                exitTransMsg();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitTransMsg();
                                doTransition(STATE_CLOSED);
                                notifyOnStop();
                                enterClosed();
                                break;
                            }
                        default:
                            break;
                    }
                    break;
                }
            case STATE_TRANSDONE:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitTransDone();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_RESTART:
                            {
                                exitTransDone();
                                doTransition(STATE_GETBUFFER);
                                enterGetBuffer();
                                break;
                            }
                        case SIG_STOP:
                            {
                                exitTransDone();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitTransDone();
                                doTransition(STATE_CLOSED);
                                notifyOnStop();
                                enterClosed();
                                break;
                            }
                        default:
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
                        case SIG_CLOSE:
                            {
                                exitError();
                                doTransition(STATE_CLOSED);
                                enterIdle();
                                break;
                            }
                        default:
                            break;
                    }
                    break;
                }
            case STATE_CLOSED:
                {
                    switch (signal) {
                        case SIG_CLOSE:
                            {
                                // do nothing
                                break;
                            }
                        default:
                            break;
                    }
                    break;
                }
            default:
                {
                    break;
                }
        }
    }

    private void doTransition(int nextState) {
        presState = nextState;
    }

    /**
     * Receives a ByteBuffer from a source.
     *
     * @param tBuffer ByteBuffer the new buffer to be processed.
     */
    @Override
    public void receiveByteBuffer(ByteBuffer tBuffer) {
        try {
            if (tBuffer.getInt(0) != tBuffer.limit()) {
                if (tBuffer.getInt(0) > tBuffer.capacity()) {
                    throw new RuntimeException("ByteBuffer corrupted - rec length: "
                            + tBuffer.getInt(0) + " capacity: " + tBuffer.capacity());
                }
                tBuffer.limit(tBuffer.getInt(0));
            }
            outputQueue.put(tBuffer);
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException(ie);
        }
        flushOutQueue();
    }

    /**
     * This channel will never be paused
     *
     * @return <tt>false</tt> always
     */
    public boolean isOutputPaused() {
        return false;
    }

    /**
     * Are there records waiting to be written?
     *
     * @return <tt>true</tt> if the output queue is not empty
     */
    @Override
    public boolean isOutputQueued() {
        return !outputQueue.isEmpty();
    }

    private static String getStateName(int state) {
        final String name;
        switch (state) {
            case STATE_IDLE:
                name = STATE_IDLE_NAME;
                break;
            case STATE_GETBUFFER:
                name = STATE_GETBUFFER_NAME;
                break;
            case STATE_TRANSMSG:
                name = STATE_TRANSMSG_NAME;
                break;
            case STATE_TRANSDONE:
                name = STATE_TRANSDONE_NAME;
                break;
            case STATE_ERROR:
                name = STATE_ERROR_NAME;
                break;
            case STATE_CLOSED:
                name = STATE_CLOSED_NAME;
                break;
            default:
                name = "UNKNOWN#" + state;
                break;
        }
        return name;
    }

    private static String getSignalName(int signal) {
        final String name;
        switch (signal) {
            case SIG_IDLE:
                name = "IDLE";
                break;
            case SIG_GET_BUFFER:
                name = "GET_BUFFER";
                break;
            case SIG_TRANSMIT:
                name = "TRANSMIT";
                break;
            case SIG_DONE:
                name = "DONE";
                break;
            case SIG_RESTART:
                name = "RESTART";
                break;
            case SIG_STOP:
                name = "STOP";
                break;
            case SIG_ERROR:
                name = "ERROR";
                break;
            case SIG_CLOSE:
                name = "CLOSE";
                break;
            default:
                name = "UNKNOWN#" + signal;
                break;
        }
        return name;
    }

    /**
     * Get the number of buffers queued for output.
     *
     * @return number of queued buffers
     */
    public int getDepth()
    {
        return outputQueue.getDepth();
    }
}
