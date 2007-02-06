/*
 * class: PayloadInputEngine
 *
 * Version $Id: PayloadInputEngine.java,v 1.24 2006/02/08 11:28:02 toale Exp $
 *
 * Date: May 19 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.NormalState;
import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.ErrorState;
import EDU.oswego.cs.dl.util.concurrent.*;

import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.BindException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class represents the input engine and it is responsible for managing
 * a collection a PayloadReceiveChannels
 *
 * @author mcp
 * @version $Id: PayloadInputEngine.java,v 1.24 2006/02/08 11:28:02 toale Exp $
 */
public class PayloadInputEngine implements DAQComponentInputProcessor, DAQComponentObserver, Runnable {

    protected static final int STATE_IDLE = 0;
    protected static final int STATE_RUNNING = 1;
    protected static final int STATE_STOPPING = 2;
    protected static final int STATE_DESTROYED = 3;
    protected static final int STATE_DISPOSING = 4;
    protected static final int STATE_ERROR = 5;

    protected static final int SIG_IDLE = 0;
    protected static final int SIG_START = 1;
    protected static final int SIG_STOP = 2;
    protected static final int SIG_DONE = 3;
    protected static final int SIG_DESTROY = 4;
    protected static final int SIG_DISPOSE = 5;
    protected static final int SIG_ERROR = 6;
    protected static final int SIG_FORCED_STOP = 7;

    public static final String STATE_IDLE_NAME = "Idle";
    public static final String STATE_RUNNING_NAME = "Running";
    public static final String STATE_STOPPING_NAME = "Stopping";
    public static final String STATE_DESTROYED_NAME = "Destroyed";
    public static final String STATE_DISPOSING_NAME = "Disposing";
    public static final String STATE_ERROR_NAME = "Error";

    private static final String[] STATE_NAME_MAP = {STATE_IDLE_NAME,
                                                    STATE_RUNNING_NAME,
                                                    STATE_STOPPING_NAME,
                                                    STATE_DESTROYED_NAME,
                                                    STATE_DISPOSING_NAME,
                                                    STATE_ERROR_NAME};
    // print log messages with state change information?
    private static final boolean TRACE_STATE = false;
    // print log messages with for server thread
    private static final boolean TRACE_THREAD = false;
    // selector timeout in msec.
    private static final int DEFAULT_SELECTOR_TIMEOUT_MSEC = 200;
    // sempahore to be used as a mutex to lock out cuncurrent ops
    // between wait code and channel locator callbacks
    protected Mutex stateMachineMUTEX = new Mutex();
    // our internal state
    protected int presState;
    // our last state
    private int prevState;
    // count transitions
    private int transitionCnt = 0;
    // count illeagal transition requests
    private int illegalTransitionCnt = 0;
    // component type of creator
    private String componentType;
    // component ID of creator
    private int componentID;
    // component fcn
    private String componentFcn;

    // selector for private use
    protected Selector selector;
    // stop flag for thread
    protected SynchronizedBoolean stopFlag;
    // destroy flag for thread
    protected SynchronizedBoolean killFlag;
    // isDisposing flag for selector code
    protected SynchronizedBoolean isDisposingFlag;
    // list of payload objects for startDisposing method call
    protected SyncCollection payloadStartDisposingSyncList;
    // isRunning flag for selector code
    protected SynchronizedBoolean isRunningFlag;
    // list of payload objects for startEngine method call
    protected SyncCollection payloadStartEngineSyncList;
    // simulated error flag;
    protected boolean simulatedError;
    // collection of individual payload engines
    protected SyncCollection payloadEngineList =
        new SyncCollection(new ArrayList(), new Semaphore(1));
    // selector timeout
    protected long selectorTimeoutMsec = DEFAULT_SELECTOR_TIMEOUT_MSEC;
    // stop notification counter
    protected int stopNotificationCounter;
    // error flag used internally to drive stop call back status
    protected boolean stopNotificiationStatus = true;
    // base name to use for payload engine call backs
    protected String payloadEngineBase = "payloadInputEngine:";
    // base numerical ID for payload engine
    protected int payloadEngineNum = 0;
    // public semaphore to indicate completed receives
    public Semaphore inputAvailable;
    // thread for this instance
    private Thread inputEngine;

    private DAQComponentObserver compObserver;
    // hack around a Linux bug -- see startServer() for explanation
    protected SynchronizedBoolean pauseThread;

    // set up logging channel for this component
    private Log log = LogFactory.getLog(PayloadInputEngine.class);

    // server port
    private int port = Integer.MIN_VALUE;
    // server byte buffer
    private IByteBufferCache serverCache;

    // reverse connection list
    private ArrayList reverseConnList = new ArrayList();
    // <tt>true</tt> if reverse connections have been made
    private boolean madeReverseConnections;

    private boolean debug = false;

    /**
     * Backward compatible constructor.
     * @deprecated DAQdataChannels are no longer supported
     */
    public PayloadInputEngine(Object unused,
                              String type,
                              int id,
                              String fcn) {
        this(type, id, fcn);

        try {
            throw new RuntimeException("Using deprecated PayloadInputEngine" +
                                       " constructor");
        } catch (RuntimeException re) {
            log.error("FIX ME", re);
        }
    }

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public PayloadInputEngine(String type,
                              int id,
                              String fcn) {
        componentType = type;
        componentID = id;
        componentFcn = fcn;
        // create sync flags
        stopFlag = new SynchronizedBoolean(true);
        killFlag = new SynchronizedBoolean(false);
        isRunningFlag = new SynchronizedBoolean(false);
        isDisposingFlag = new SynchronizedBoolean(false);
        payloadStartEngineSyncList = new SyncCollection(new ArrayList(),
                new Semaphore(1));
        payloadStartDisposingSyncList = new SyncCollection(new ArrayList(),
                new Semaphore(1));
        inputAvailable = new Semaphore(0);
        // create new state info
        presState = STATE_IDLE;
        prevState = presState;
        // create selector
        try {
            selector = Selector.open();
        } catch (IOException ioe) {
            transition(SIG_ERROR);
            log.error("Problem on selector.open(): ", ioe);
            throw new RuntimeException(ioe);
        }
        pauseThread = new SynchronizedBoolean(false);
        inputEngine = new Thread(this);
        inputEngine.setName("PayloadInputEngine-" + type + "-id#" + id +
                "-" + fcn);
    }

    // thread start up is separated from constructor for good practice.
    // Needs to be called immediately after constructor.
    public void start() {
        inputEngine.start();
    }

    public void startServer(IByteBufferCache serverCache) throws IOException {
        // hack around Linux kernel bug:
        //     ssChan.register() blocks for an indefinite amount of time if
        //     called while a select is in progress, so we pause the thread,
        //     register the server socket, and then resume the thread
        //
        synchronized (pauseThread) {
            if (pauseThread.get()) {
                log.error("Thread is already paused -- this could be bad!");
            }

            // indicate to worker thread that it should pause
            pauseThread.set(true);

            // wait for thread to notify us that it has paused
            try {
                pauseThread.wait();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        ServerSocketChannel ssChan = ServerSocketChannel.open();
        ssChan.configureBlocking(false);

        ssChan.socket().bind(null);
        port = ssChan.socket().getLocalPort();

        ssChan.register(selector, SelectionKey.OP_ACCEPT);

        synchronized (pauseThread) {
            // turn off pause flag
            if (!pauseThread.get()) {
                log.error("Expected thread to be paused!");
            } else {
                pauseThread.set(false);
            }

            // let the thread know that we're done
            pauseThread.notify();
        }

        this.serverCache = serverCache;
    }

    public int getServerPort() {
        return port;
    }

    protected void enterIdle() {
        simulatedError = false;
        stopFlag.set(true);
        isRunningFlag.set(false);
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel payload =
                (PayloadReceiveChannel) payloadListIterator.next();
            payload.stopEngine();
        }
        payloadEngineList.clear();
        madeReverseConnections = false;
    }

    protected void exitIdle() {
        stopNotificiationStatus = true;
    }

    protected void enterRunning() {
        stopFlag.set(false);
        stopNotificationCounter = payloadEngineList.size();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel payload =
                (PayloadReceiveChannel) payloadListIterator.next();
            payloadStartEngineSyncList.add(payload);
        }
        isRunningFlag.set(true);
    }

    protected void exitRunning() {
    }

    protected void enterStopping() {
        if (payloadEngineList.size() == 0) {
            transition(SIG_IDLE);
        }
    }

    protected void exitStopping() {
    }

    protected void enterDisposing() {
        stopNotificationCounter = payloadEngineList.size();
        if (stopNotificationCounter == 0) {
            // if we have no inputs, then we're done
            transition(SIG_IDLE);
            // make sure we return from here
            return;
        } else {
            Iterator payloadListIterator = payloadEngineList.iterator();
            while (payloadListIterator.hasNext()) {
                PayloadReceiveChannel payload = (PayloadReceiveChannel) payloadListIterator.next();
                payloadStartDisposingSyncList.add(payload);
            }
            isDisposingFlag.set(true);
        }
    }

    protected void exitDisposing() {
        isDisposingFlag.set(false);
    }

    protected void enterError() {
        if (compObserver != null) {
            compObserver.update(ErrorState.UNKNOWN_ERROR, DAQCmdInterface.SINK);
        }
    }

    protected void exitError() {
    }

    protected void enterDestroyed() {
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel payload =
                (PayloadReceiveChannel) payloadListIterator.next();
            payload.stopEngine();
        }
        payloadEngineList.clear();
        madeReverseConnections = false;
    }

    // instance member method (alphabetic)
    protected void notifyClient() {
        if (compObserver != null) {
            compObserver.update(NormalState.STOPPED, DAQCmdInterface.SINK);
        }
    }

    protected void localForcedStopProcessing() {
        // all forced stops are considered successful
        stopNotificiationStatus = true;
        // force back to idle.
        transition(SIG_FORCED_STOP);
    }

    public void forcedStopProcessing() {
        try {
            stateMachineMUTEX.acquire();
            localForcedStopProcessing();
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException("Forced stop interrupted", ie);
        }
    }

    public void startProcessing() {
        if (killFlag.get()) {
            throw new RuntimeException("Cannot restart destroyed engine");
        }

        try {
            makeReverseConnections();
        } catch (IOException ioe) {
            throw new RuntimeException("Cannot make reverse connections", ioe);
        }

        try {
            stateMachineMUTEX.acquire();
            transition(SIG_START);
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            stateMachineMUTEX.release();
            throw new RuntimeException("Interrupted engine start", ie);
        }
    }

    public void destroyProcessor() {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_DESTROY);
            killFlag.set(true);
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            stateMachineMUTEX.release();
            throw new RuntimeException("Cannot destroy engine", ie);
        }
    }

    public void startDisposing() {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_DISPOSE);
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            stateMachineMUTEX.release();

            final String errMsg =
                "PayloadInputEngine: unable to start disposing: ";
            throw new RuntimeException(errMsg, ie);
        }
    }

    public synchronized String getPresentState() {
        return STATE_NAME_MAP[presState];
    }

    public synchronized String[] getPresentChannelStates() {
        ArrayList stateList = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            stateList.add(msg.presentState());
        }
        return (String[]) stateList.toArray(new String[0]);
    }

    public synchronized Long[] getBytesReceived() {
        ArrayList byteCount = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            byteCount.add(new Long(msg.bytesReceived));
        }
        return (Long[]) byteCount.toArray(new Long[0]);
    }

    public synchronized Long[] getRecordsReceived() {
        ArrayList recordCount = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            recordCount.add(new Long(msg.recordsReceived));
        }
        return (Long[]) recordCount.toArray(new Long[0]);
    }

    public synchronized Long[] getStopMessagesReceived() {
        ArrayList recordCount = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            recordCount.add(new Long(msg.stopMsgReceived));
        }
        return (Long[]) recordCount.toArray(new Long[0]);
    }

    public synchronized Long[] getLimitToStopAllocation() {
        ArrayList byteLimit = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            byteLimit.add(new Long(msg.limitToStopAllocation));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getLimitToRestartAllocation() {
        ArrayList byteLimit = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            byteLimit.add(new Long(msg.limitToRestartAllocation));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getPercentMaxStopAllocation() {
        ArrayList byteLimit = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            byteLimit.add(new Long(msg.percentOfMaxStopAllocation));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getPercentMaxRestartAllocation() {
        ArrayList byteLimit = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            byteLimit.add(new Long(msg.percentOfMaxRestartAllocation));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getBufferCurrentAcquiredBytes() {
        ArrayList byteLimit = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            byteLimit.add(new Long(msg.getBufferCurrentAcquiredBytes()));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getBufferCurrentAcquiredBuffers() {
        ArrayList byteLimit = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            byteLimit.add(new Long(msg.getBufferCurrentAcquiredBuffers()));
        }

        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Boolean[] getAllocationStopped() {
        ArrayList allocationStatus = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadReceiveChannel msg =
                (PayloadReceiveChannel) payloadListIterator.next();
            allocationStatus.add(new Boolean(msg.allocationStopped));
        }
        return (Boolean[]) allocationStatus.toArray(new Boolean[0]);
    }

    public boolean isRunning() {
        return (presState == STATE_RUNNING);
    }

    public boolean isStopped() {
        return (presState == STATE_IDLE);
    }

    public boolean isError() {
        return (presState == STATE_ERROR);
    }

    public boolean isDisposing() {
        return (presState == STATE_DISPOSING);
    }

    public boolean isDestroyed() {
        return (presState == STATE_DESTROYED);
    }

    public boolean isHealthy() {
        boolean allStopped = true;
        Boolean[] stopped = getAllocationStopped();
        for (int i = 0; i < stopped.length; i++) {
            allStopped &= stopped[i].booleanValue();
        }
        return !allStopped;
    }

    PayloadReceiveChannel createReceiveChannel(String name,
                                               ReadableByteChannel channel,
                                               IByteBufferCache bufMgr)
    {
        return new PayloadReceiveChannel(name, selector, channel, bufMgr,
                                         inputAvailable);
    }

    public PayloadReceiveChannel addDataChannel(ReadableByteChannel channel,
                                                IByteBufferCache bufMgr)
    {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }

        if (presState != STATE_IDLE) {
            final String errMsg = "Cannot add data channel while engine is " +
                getPresentState();
            throw new RuntimeException(errMsg);
        }

        payloadEngineNum++;

        String recvName;
        if (componentType == null) {
            recvName = payloadEngineBase + payloadEngineNum;
        } else {
            recvName = componentType + ":" + componentFcn + ":" +
                    payloadEngineNum;
        }

        PayloadReceiveChannel payload =
            createReceiveChannel(recvName, channel, bufMgr);
        payload.registerComponentObserver(this, recvName);

        payloadEngineList.add(payload);
        stateMachineMUTEX.release();
        return payload;
    }

    public void injectError() {
        simulatedError = true;
    }

    public void injectLowLevelError() {
        try {
            stateMachineMUTEX.acquire();
            if (payloadEngineList.size() == 0) {
                injectError();
            } else {
                Iterator payloadListIterator = payloadEngineList.iterator();
                while (payloadListIterator.hasNext()) {
                    PayloadReceiveChannel payload =
                            (PayloadReceiveChannel) payloadListIterator.next();
                    payload.injectError();
                }
            }
        } catch (InterruptedException ie) {
            log.error(ie);

            final String errMsg = "Low level error injection interrupted";
            throw new RuntimeException(errMsg, ie);
        }
    }

    public void trafficStopNotification(String notificationID) {
        if (presState == STATE_RUNNING) {
            stopNotificationCounter--;
            if (stopNotificationCounter <= 0) {
                localForcedStopProcessing();
            }
        }
        if (presState == STATE_DISPOSING) {
            stopNotificationCounter--;
            if (stopNotificationCounter <= 0) {
                // all payload engines have timed out ...
                // hope all data has been disposed of
                transition(SIG_IDLE);
            }
        }
    }

    public void trafficErrorNotification(String notificationID) {
        try {
            stateMachineMUTEX.acquire();
            stopNotificiationStatus = false;
            transition(SIG_ERROR);
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            // not much to do here
            stateMachineMUTEX.release();
            log.error(ie);
        }
    }

    public void registerComponentObserver(DAQComponentObserver compObserver) {
        this.compObserver = compObserver;
    }

    private void addSocketChannel(SocketChannel chan)
        throws IOException
    {
        addSocketChannel(chan, serverCache);
    }

    private PayloadReceiveChannel addSocketChannel(SocketChannel chan,
                                                   IByteBufferCache bufCache)
        throws IOException
    {
        // disable blocking or receive engine will die
        chan.configureBlocking(false);
        return addDataChannel(chan, bufCache);
    }

    public void update(Object object, String notificationID)
    {
        if (object == NormalState.STOPPED) {
            trafficStopNotification(notificationID);
        } else {
            log.error("Got " + object + " from " + notificationID);
        }
    }

    public void addReverseConnection(String hostName, int port,
                                     IByteBufferCache bufCache)
        throws IOException
    {
        synchronized (reverseConnList) {
            ReverseConnection rConn =
                new ReverseConnection(hostName, port, bufCache);
            reverseConnList.add(rConn);
        }
    }

    void makeReverseConnections()
        throws IOException
    {
        if (!madeReverseConnections) {
            IOException deferred = null;
            for (Iterator iter = reverseConnList.iterator(); iter.hasNext(); ) {
                ((ReverseConnection) iter.next()).connect();
            }
            madeReverseConnections = true;
        }
    }

    public void run() {
        for (; ;) {
            if (TRACE_THREAD) System.err.println("RunTop");
            // now, kick everyones timer entry
            Iterator payloadListIterator = payloadEngineList.iterator();
            while (payloadListIterator.hasNext()) {
                PayloadReceiveChannel payload =
                    (PayloadReceiveChannel) payloadListIterator.next();
                payload.processTimer();
            }

            payloadListIterator = payloadStartEngineSyncList.iterator();
            while (payloadListIterator.hasNext()) {
                PayloadReceiveChannel payload =
                    (PayloadReceiveChannel) payloadListIterator.next();
                payload.startEngine();
                payloadListIterator.remove();
            }

            payloadListIterator = payloadStartDisposingSyncList.iterator();
            while (payloadListIterator.hasNext()) {
                PayloadReceiveChannel payload =
                    (PayloadReceiveChannel) payloadListIterator.next();
                payload.startDisposing();
                payloadListIterator.remove();
            }

            // if startServer() wants thread to pause...
            if (pauseThread.get()) {
                synchronized (pauseThread) {
                    // notify startServer() that we've paused
                    pauseThread.notify();

                    // wait for startServer() to register server socket
                    try {
                        pauseThread.wait();
                    } catch (Exception ex) {
                        log.error("Thread pause was interrupted", ex);
                    }
                }

                if (pauseThread.get()) {
                    log.error("'pauseThread' set to true after pause");
                }
            }

            // process selects
            int numSelected;
            try {
                numSelected = selector.select(selectorTimeoutMsec);
            } catch (IOException ioe) {
                log.error("Error on selection: ", ioe);
                numSelected = 0;
            }
            if (TRACE_THREAD) System.err.println("RunSel#" + numSelected);
            if (numSelected != 0) {
                // get iterator for select keys
                Iterator selectorIterator =
                    selector.selectedKeys().iterator();
                while (selectorIterator.hasNext()) {
                    // get the selection key
                    SelectionKey selKey =
                        (SelectionKey) selectorIterator.next();

                    if (selKey.isAcceptable()) {
                        selectorIterator.remove();

                        ServerSocketChannel ssChan =
                            (ServerSocketChannel) selKey.channel();

                        try {
                            SocketChannel chan = ssChan.accept();

                            // if server channel is non-blocking,
                            // chan may be null

                            if (chan != null) {
                                addSocketChannel(chan);
                            }

                            if (TRACE_THREAD) {
                                System.err.println("New chan is" +
                                                   (chan.isRegistered() ?
                                                    "" : " NOT") +
                                                   " registered");
                            }
                        } catch (IOException ioe) {
                            log.error("Couldn't accept client socket", ioe);
                        }
                        continue;
                    }
                    if (isRunningFlag.get() || isDisposingFlag.get()) {
                        // call the payload engine to process
                        PayloadReceiveChannel payloadEngine =
                            ((PayloadReceiveChannel) selKey.attachment());
                        selectorIterator.remove();
                        // assume that payloadEngine will deal with whatever has happened
                        if (payloadEngine == null) {
                            log.error("No payload engine found" +
                                      " for selected channel");
                        } else {
                            payloadEngine.processSelect(selKey);
                        }

                    } else {
                        // should not be getting called otherwise, cancel it
                        selKey.cancel();
                        selectorIterator.remove();
                    }
                }
            }
            if (TRACE_THREAD) {
                System.err.println("RunState=" + getPresentState());
            }
            // now we get a lock
            try {
                stateMachineMUTEX.acquire();
            } catch (InterruptedException ie) {
                log.error(ie);
                throw new RuntimeException(ie);
            }
            // do a simulated error?
            if (simulatedError) {
                if (TRACE_THREAD) System.err.println("RunSimError");
                stopNotificiationStatus = false;
                transition(SIG_ERROR);
                simulatedError = false;
                isRunningFlag.set(false);
            } else if (presState == STATE_RUNNING) {
                // check if we are stopping
                if (stopFlag.get()) {
                    transition(SIG_STOP);
                    if (TRACE_THREAD) System.err.println("RunStopping");
                }
            } else if (presState == STATE_STOPPING) {
                // all done, so set state and return
                transition(SIG_DONE);
                if (TRACE_THREAD) System.err.println("RunDone");
                isRunningFlag.set(false);
            } else {
                stopFlag.set(true);
                if (TRACE_THREAD) System.err.println("RunStop");
                isRunningFlag.set(false);
            }
            stateMachineMUTEX.release();

            if (killFlag.get()) {
                try {
                    if (TRACE_THREAD) System.err.println("RunKilled");
                    selector.close();
                } catch (IOException ioe) {
                    // we're bailing now, so not much to do
                }
                // exit the thread
                break;
            }
        }
    }

    // instance member method (alphabetic)
    protected void transition(int signal) {
        if (log.isDebugEnabled() && TRACE_STATE) {
            log.debug("PayloadInputEngine " +
                    componentType + ":" + componentFcn +
                    " state " + getStateName(presState) +
                    " transition signal " + getSignalName(signal));
        }
        if (debug) {
            System.err.println(componentType + ":" + componentFcn + " " +
                               getStateName(presState) + " -> " +
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
                        case SIG_START:
                            {
                                exitIdle();
                                doTransition(STATE_RUNNING);
                                enterRunning();
                                break;
                            }
                        case SIG_DESTROY:
                            {
                                exitIdle();
                                doTransition(STATE_DESTROYED);
                                enterDestroyed();
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
            case STATE_RUNNING:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitRunning();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_IDLE:
                            {
                                exitRunning();
                                doTransition(STATE_IDLE);
                                notifyClient();
                                enterIdle();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitRunning();
                                doTransition(STATE_IDLE);
                                notifyClient();
                                enterIdle();
                                break;
                            }
                        case SIG_STOP:
                            {
                                exitRunning();
                                doTransition(STATE_IDLE);
                                enterStopping();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_STOPPING:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitStopping();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_DONE:
                            {
                                exitStopping();
                                doTransition(STATE_IDLE);
                                notifyClient();
                                enterIdle();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitStopping();
                                doTransition(STATE_IDLE);
                                notifyClient();
                                enterIdle();
                                break;
                            }
                        case SIG_IDLE:
                            {
                                exitStopping();
                                doTransition(STATE_IDLE);
                                notifyClient();
                                enterIdle();
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
                                notifyClient();
                                enterIdle();
                                break;
                            }
                        case SIG_FORCED_STOP:
                            {
                                exitDisposing();
                                doTransition(STATE_IDLE);
                                notifyClient();
                                enterIdle();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_DESTROYED:
                {
                    switch (signal) {
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
            case STATE_RUNNING:
                name = STATE_RUNNING_NAME;
                break;
            case STATE_STOPPING:
                name = STATE_STOPPING_NAME;
                break;
            case STATE_DESTROYED:
                name = STATE_DESTROYED_NAME;
                break;
            case STATE_DISPOSING:
                name = STATE_DISPOSING_NAME;
                break;
            case STATE_ERROR:
                name = STATE_ERROR_NAME;
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
            case SIG_START:
                name = "START";
                break;
            case SIG_STOP:
                name = "STOP";
                break;
            case SIG_DONE:
                name = "DONE";
                break;
            case SIG_DESTROY:
                name = "DESTROY";
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
                "PayloadInputEngine " +
                componentType + ":" + componentFcn +
                " illegal transition for state " +
                getStateName(presState) +
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

    /**
     * An internet port which input engine needs to connect to in order to
     * receive data.
     */
    class ReverseConnection
    {
        private String hostName;
        private int port;
        private IByteBufferCache bufCache;

        ReverseConnection(String hostName, int port, IByteBufferCache bufCache)
        {
            this.hostName = hostName;
            this.port = port;
            this.bufCache = bufCache;
        }

        void connect()
            throws IOException
        {
            SocketChannel sock =
                SocketChannel.open(new InetSocketAddress(hostName, port));

            addSocketChannel(sock, bufCache);
        }
    }
}
