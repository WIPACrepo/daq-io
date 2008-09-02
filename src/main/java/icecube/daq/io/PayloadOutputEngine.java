/*
 * class: PayloadOutputEngine
 *
 * Version $Id: PayloadOutputEngine.java 3439 2008-09-02 17:08:41Z dglo $
 *
 * Date: May 19 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.Mutex;
import EDU.oswego.cs.dl.util.concurrent.Semaphore;
import EDU.oswego.cs.dl.util.concurrent.SyncCollection;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedBoolean;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class represents the output engine and it is responsible for managing
 * a collection a PayloadTransmitChannels
 *
 * @author mcp
 * @version $Id: PayloadOutputEngine.java 3439 2008-09-02 17:08:41Z dglo $
 */
public class PayloadOutputEngine implements DAQComponentObserver, DAQComponentOutputProcess, Runnable {

    // private static final member data
    private static final int STATE_IDLE = 0;
    private static final int STATE_RUNNING = 1;
    private static final int STATE_STOPPING = 2;
    private static final int STATE_DESTROYED = 3;
    private static final int STATE_ERROR = 4;

    private static final int SIG_IDLE = 0;
    private static final int SIG_START = 1;
    private static final int SIG_STOP = 2;
    private static final int SIG_DONE = 3;
    private static final int SIG_DESTROY = 4;
    private static final int SIG_ERROR = 5;
    private static final int SIG_FORCED_STOP = 6;

    public static final String STATE_IDLE_NAME = "Idle";
    public static final String STATE_RUNNING_NAME = "Running";
    public static final String STATE_STOPPING_NAME = "Stopping";
    public static final String STATE_DESTROYED_NAME = "Destroyed";
    public static final String STATE_ERROR_NAME = "Error";

    private static final String[] STATE_NAME_MAP = {STATE_IDLE_NAME,
                                                    STATE_RUNNING_NAME,
                                                    STATE_STOPPING_NAME,
                                                    STATE_DESTROYED_NAME,
                                                    STATE_ERROR_NAME};
    // selector timeout in msec.
    private static final int DEFAULT_SELECTOR_TIMEOUT_MSEC = 250;

    // print log messages with state change information?
    private static final boolean TRACE_STATE = false;

    // sempahore to be used as a mutex to lock out cuncurrent ops
    // between wait code and channel locator callbacks
    private Mutex stateMachineMUTEX = new Mutex();
    // our internal state
    private int presState;
    // component type of creator
    private String componentType;
    // component ID of creator
    private int componentID;
    // component fcn
    private String componentFcn;

    // selector for private use
    private Selector selector;
    // destroy flag for thread
    private SynchronizedBoolean killFlag;
    // isRunning flag for selector code
    private SynchronizedBoolean isRunningFlag;
    // list of payload objects for startEngine method call
    private SyncCollection payloadStartEngineSyncList;
    // simulated error flag;
    private boolean simulatedError;
    // collection of individual payload engines
    private ArrayList payloadEngineList = new ArrayList();
    // ID to differentiate this notification from all others
    private String payloadApplicationStopNotificationID = "";
    // stop notification counter
    private int stopNotificationCounter;

    // base numerical ID for payload engine
    private int payloadEngineNum = 0;
    // thread for this instance
    private Thread outputEngine;

    private DAQComponentObserver compObserver;

    private boolean debug = false;

    private boolean isConnected = false;

    // set up logging channel for this component
    private Log log = LogFactory.getLog(PayloadOutputEngine.class);

    /**
     * Backward compatible constructor.
     * @deprecated DAQdataChannels are no longer supported
     */
    public PayloadOutputEngine(Object unused,
                               String type,
                               int id,
                               String fcn) {
        this(type, id, fcn);

        try {
            throw new RuntimeException("Using deprecated PayloadOutputEngine" +
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
    public PayloadOutputEngine(String type,
                               int id,
                               String fcn) {
        componentType = type;
        componentID = id;
        componentFcn = fcn;
        // create sync flags
        killFlag = new SynchronizedBoolean(false);
        isRunningFlag = new SynchronizedBoolean(false);
        payloadStartEngineSyncList = new SyncCollection(new ArrayList(),
                new Semaphore(1));
        // create new state info
        presState = STATE_IDLE;

        // create selector
        try {
            selector = Selector.open();
        } catch (IOException ioe) {
            // oops---really bad.
            transition(SIG_ERROR);
            log.error("Problem on Selector: ", ioe);
            throw new RuntimeException(ioe);
        }
        outputEngine = new Thread(this);
        outputEngine.setName("PayloadOutputEngine-" + type + "-id#" + id +
                "-" + fcn);
    }

    // thread start up is separated from constructor for good practice.
    // Needs to be called immediately after constructor.
    public void start() {
        outputEngine.start();
    }

    public String getComponentType() {
        return componentType;
    }

    public int getComponentId() {
        return componentID;
    }

    public String getComponentFcn() {
        return componentFcn;
    }

    public void registerComponentObserver(DAQComponentObserver compObserver) {
        this.compObserver = compObserver;
    }

    protected void enterIdle() {
        simulatedError = false;
        //stopFlag.set(true);
        isRunningFlag.set(false);
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel payload = (PayloadTransmitChannel) payloadListIterator.next();
            payload.stopEngine();
        }
    }

    protected void exitIdle() {
    }

    protected void enterRunning() {
        //stopFlag.set(false);
        stopNotificationCounter = payloadEngineList.size();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel payload = (PayloadTransmitChannel) payloadListIterator.next();
            payloadStartEngineSyncList.add(payload);
        }
        isRunningFlag.set(true);
    }

    protected void exitRunning() {
    }

    protected void enterStopping() {
        if (payloadEngineList.size() == 0) {
            trafficStopNotification(payloadApplicationStopNotificationID);
        } else {
            Iterator payloadListIterator = payloadEngineList.iterator();
            while (payloadListIterator.hasNext()) {
                PayloadTransmitChannel payload = (PayloadTransmitChannel) payloadListIterator.next();
                payload.sendLastAndStop();
            }
        }
    }

    protected void exitStopping() {
    }

    protected void enterError() {
        if (compObserver != null) {
            compObserver.update(ErrorState.UNKNOWN_ERROR, DAQCmdInterface.SOURCE);
        }
    }

    protected void exitError() {

    }

    protected void enterDestroyed() {
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel payload = (PayloadTransmitChannel) payloadListIterator.next();
            payload.stopEngine();
        }
    }

    public void forcedStopProcessing() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        // forced stop is always succussful
        transition(SIG_FORCED_STOP);
        stateMachineMUTEX.release();
    }

    public void startProcessing() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        if (presState == STATE_IDLE) {
            transition(SIG_START);
        } else {
            stateMachineMUTEX.release();
            throw new RuntimeException("PayloadOutputEngine: cannot restart engine");
        }
        stateMachineMUTEX.release();
    }

    private void notifyClient() {
        if (compObserver != null) {
            compObserver.update(NormalState.STOPPED, DAQCmdInterface.SOURCE);
        }
    }

    public void destroyProcessor() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        if (presState == STATE_IDLE) {
            transition(SIG_DESTROY);
            killFlag.set(true);
        } else {
            stateMachineMUTEX.release();
            throw new RuntimeException("PayloadOutputEngine: unable to destroy engine.");
        }
        stateMachineMUTEX.release();
    }

    public synchronized String getPresentState() {
        return STATE_NAME_MAP[presState];
    }

    public void clearError() {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_IDLE);
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException(ie);
        }
    }

    public QueuedOutputChannel connect(IByteBufferCache bufCache,
                                       WritableByteChannel chan, int srcId)
        throws IOException
    {
        return addDataChannel(chan, bufCache);
    }

    public void disconnect()
        throws IOException
    {
        enterIdle();

        IOException ioEx = null;

        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel payload =
                (PayloadTransmitChannel) payloadListIterator.next();
            try {
                payload.close();
            } catch (IOException ioe) {
                ioEx = ioe;
            }
            payloadListIterator.remove();
        }

        if (ioEx != null) {
            throw ioEx;
        }
        isConnected = false;
    }

    public synchronized String[] getPresentChannelStates() {
        ArrayList stateList = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel msg = (PayloadTransmitChannel) payloadListIterator.next();
            stateList.add(msg.presentState());
        }
        return (String[]) stateList.toArray(new String[0]);
    }

    public synchronized Long[] getDepth() {
        ArrayList recordCount = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel msg =
                (PayloadTransmitChannel) payloadListIterator.next();
            recordCount.add(new Long(msg.getDepth()));
        }
        return (Long[]) recordCount.toArray(new Long[0]);
    }

    public synchronized Long[] getBytesSent() {
        ArrayList byteCount = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel msg = (PayloadTransmitChannel) payloadListIterator.next();
            byteCount.add(new Long(msg.bytesSent));
        }
        return (Long[]) byteCount.toArray(new Long[0]);
    }


    public synchronized long[] getRecordsSent() {
        long[] recordCount = new long[payloadEngineList.size()];
        for (int i = 0; i < recordCount.length; i++) {
            PayloadTransmitChannel msg =
                (PayloadTransmitChannel) payloadEngineList.get(i);
            recordCount[i] = msg.recordsSent;
        }
        return recordCount;
    }

    public synchronized Long[] getStopMessagesSent() {
        ArrayList recordCount = new ArrayList();
        Iterator payloadListIterator = payloadEngineList.iterator();
        while (payloadListIterator.hasNext()) {
            PayloadTransmitChannel msg = (PayloadTransmitChannel) payloadListIterator.next();
            recordCount.add(new Long(msg.stopMsgSent));
        }
        return (Long[]) recordCount.toArray(new Long[0]);
    }

    /**
     * Return the single channel associated with this output engine.
     *
     * @return output channel
     *
     * @throws Error if there is more than one output channel
     */
    public synchronized OutputChannel getChannel() {
        if (payloadEngineList.size() != 1) {
            throw new Error("Engine should only contain one channel, not " +
                            payloadEngineList.size());
        }

        return (PayloadTransmitChannel) payloadEngineList.get(0);
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

    public boolean isDestroyed() {
        return (presState == STATE_DESTROYED);
    }

    public QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                              IByteBufferCache bufMgr)
    {
        if (presState != STATE_IDLE) {
            throw new RuntimeException("Cannot add data channel while engine is " +
                    getPresentState());
        }

        payloadEngineNum++;

        String xmitName;
        if (componentType == null) {
            xmitName = "payloadOutputEngine:" + payloadEngineNum;
        } else {
            xmitName = componentType + ":" + componentFcn + ":" +
                    payloadEngineNum;
        }

        PayloadTransmitChannel payload =
                new PayloadTransmitChannel(xmitName, channel, selector, bufMgr);
        payload.registerComponentObserver(this, xmitName);

        payloadEngineList.add(payload);
        isConnected = true;

        return payload;
    }

    public boolean isConnected(){
        return isConnected;
    }

    public void registerStopNotificationCallback(String notificationID) {
        payloadApplicationStopNotificationID = notificationID;
    }

    public void registerErrorNotificationCallback(String notificationID) {
    }

    public void sendLastAndStop() {
        if (debug && log.isInfoEnabled()) {
            log.info(componentType + ":" + componentFcn +" -- sendLastAndStop");
        }
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        transition(SIG_STOP);
        stateMachineMUTEX.release();
        if (compObserver != null) {
            compObserver.update(NormalState.STOPPED, DAQCmdInterface.SOURCE);
        }
    }

    public void injectError() {
        simulatedError = true;
    }

    public void injectLowLevelError() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
        }
        if (payloadEngineList.size() == 0) {
            this.injectError();
        } else {
            Iterator payloadListIterator = payloadEngineList.iterator();
            while (payloadListIterator.hasNext()) {
                PayloadTransmitChannel payload = (PayloadTransmitChannel) payloadListIterator.next();
                payload.injectError();
            }
        }
        stateMachineMUTEX.release();
    }

    public void trafficStopNotification(String notificationID) {
        if (presState == STATE_STOPPING) {
            stopNotificationCounter--;
            if (stopNotificationCounter <= 0) {
                transition(SIG_DONE);
                isRunningFlag.set(false);
            }
        }
    }

    public void trafficErrorNotification(String notificationID) {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_ERROR);
            stateMachineMUTEX.release();
        } catch (InterruptedException e) {
            // not much to do here
            stateMachineMUTEX.release();
        }
    }

    public void run() {
        int numSelected = 0;

        for (; ;) {
            try {
                numSelected = selector.select(DEFAULT_SELECTOR_TIMEOUT_MSEC);
            } catch (IOException ioe) {
                log.error(ioe);
                throw new RuntimeException(ioe);
            }
            // now, kick everyones timer entry
            Iterator payloadListIterator = payloadEngineList.iterator();
            while (payloadListIterator.hasNext()) {
                PayloadTransmitChannel payload = (PayloadTransmitChannel) payloadListIterator.next();
                payload.processTimer();
            }
            // and deal with posted start commands
            if (!payloadStartEngineSyncList.isEmpty()) {
                payloadListIterator = payloadStartEngineSyncList.iterator();
                while (payloadListIterator.hasNext()) {
                    PayloadTransmitChannel payload = (PayloadTransmitChannel) payloadListIterator.next();
                    payload.startEngine();
                    payloadListIterator.remove();
                }
            }
            if (numSelected != 0) {
                // get iterator for select keys
                Set keys = selector.selectedKeys();
                Iterator selectorIterator = keys.iterator();
                while (selectorIterator.hasNext()) {
                    // get the selection key
                    SelectionKey selKey = (SelectionKey) selectorIterator.next();
                    if (isRunningFlag.get()) {
                        // call the payload engine to process
                        PayloadTransmitChannel payloadChannel =
                                ((PayloadTransmitChannel) selKey.attachment());
                        selectorIterator.remove();
                        // assume that payloadChannel will deal with whatever has happened
                        payloadChannel.processSelect(selKey);
                    } else {
                        // should not be getting called otherwise, cancel it
                        selKey.cancel();
                        selectorIterator.remove();
                    }
                }
            }
            // lock access
            try {
                stateMachineMUTEX.acquire();
            } catch (InterruptedException ie) {
            }
            // do a simulated error?
            if (simulatedError) {
                transition(SIG_ERROR);
                simulatedError = false;
                isRunningFlag.set(false);
            } else if (presState == STATE_RUNNING) {

            } else if (presState == STATE_STOPPING) {

            } else {

            }
            stateMachineMUTEX.release();

            if (killFlag.get()) {
                try {
                    selector.close();
                } catch (IOException ioe) {
                    log.error(ioe);
                    throw new RuntimeException(ioe);
                }
                // exit the thread
                return;
            }
        }
    }

// instance member method (alphabetic)
    private void transition(int signal) {
         if (log.isDebugEnabled() && TRACE_STATE) {
            log.debug("PayloadOutputEngine " +
                      componentType + ":" + componentFcn +
                      " state " + getStateName(presState) +
                      " transition signal " + getSignalName(signal));
        } else if (debug && log.isInfoEnabled()) {
            log.info(componentType + ":" + componentFcn + " " +
                     getStateName(presState) + " -> " + getSignalName(signal));
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
                        default:
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
                                doTransition(STATE_STOPPING);
                                enterStopping();
                                break;
                            }
                        default:
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
                        case SIG_IDLE:
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
                        default:
                            break;
                    }
                    break;
                }
            case STATE_DESTROYED:
                {
                    switch (signal) {
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

    public void update(Object object, String notificationID)
    {
        if (object == NormalState.STOPPED) {
            trafficStopNotification(notificationID);
        } else {
            log.error("Got " + object + " from " + notificationID);
        }
    }

    private static String getStateName(int state){
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
        case STATE_ERROR:
            name = STATE_ERROR_NAME;
            break;
        default:
            name = "UNKNOWN#" + state;
            break;
        }
        return name;
    }

    private static String getSignalName(int signal){
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
}
