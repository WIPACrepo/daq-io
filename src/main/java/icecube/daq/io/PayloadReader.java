package icecube.daq.io;

import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.NormalState;
import icecube.daq.common.DAQCmdInterface;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

import java.net.InetSocketAddress;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class PayloadReader
    implements DAQComponentInputProcessor, InputChannelParent, Runnable
{
    class Flag
    {
        private String name;
        private boolean flag;

        Flag(String name)
        {
            this.name = name;
        }

        synchronized void clear()
        {
            flag = false;
        }

        boolean isSet()
        {
            return flag;
        }

        synchronized void set()
        {
            flag = true;
        }

        public String toString()
        {
            return (flag ? "" : "!") + name;
        }
    }

    /** default input buffer size */
    static final int DEFAULT_BUFFER_SIZE = 2048;

    /** logging object */
    private static final Log LOG = LogFactory.getLog(PayloadReader.class);

    /** selector timeout (in msec.) */
    private static final int SELECTOR_TIMEOUT = 1000;

    /** run states */
    private enum RunState {
        CREATED, IDLE, RUNNING, DISPOSING, DESTROYED, ERROR
    }

    /** reader name */
    private String name;
    /** input buffer size */
    private int bufferSize;

    /** worker thread */
    private Thread thread;
    /** input socket selector */
    protected Selector selector;
    /** current state */
    private RunState state;
    /** new state */
    private RunState newState;
    /** Observer to notify on stop/error */
    private DAQComponentObserver compObserver;

    /** list of recently added channels */
    private ArrayList<InputChannel> newChanList =
        new ArrayList<InputChannel>();
    /** list of active channels */
    private ArrayList<InputChannel> chanList =
        new ArrayList<InputChannel>();

    private Flag channelStopFlag = new Flag("channelStop");
    /** exclusive lock used when changing reader state */
    private Object stateLock = new Object();

    // server port
    private int port = Integer.MIN_VALUE;
    // hack around a Linux bug -- see startServer() for explanation
    protected Flag pauseThread = new Flag("pauseThread");
    // server byte buffer
    private IByteBufferCache serverCache;

    // reverse connection list
    private ArrayList<ReverseConnection> reverseConnList =
        new ArrayList<ReverseConnection>();
    // <tt>true</tt> if reverse connections have been made
    private boolean madeReverseConnections;

    public PayloadReader(String name)
    {
        this(name, DEFAULT_BUFFER_SIZE);
    }

    public PayloadReader(String name, int bufferSize)
    {
        this.name = name;
        this.bufferSize = bufferSize;

        state = RunState.CREATED;
        newState = state;
    }

    public InputChannel addDataChannel(SelectableChannel channel,
                                       IByteBufferCache bufMgr)
        throws IOException
    {
        return addDataChannel(channel, bufMgr, bufferSize);
    }

    public InputChannel addDataChannel(SelectableChannel channel,
                                       IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
final boolean DEBUG_ADD = false;
if(DEBUG_ADD)System.err.println("AddChanTop");
        if (state != RunState.IDLE) {
            final String errMsg = "Cannot add data channel while engine is " +
                getPresentState();
            throw new RuntimeException(errMsg);
        }

if(DEBUG_ADD)System.err.println("AddChanCre "+channel);
        InputChannel chanData = createChannel(channel, bufMgr, bufSize);
if(DEBUG_ADD)System.err.println("AddChan "+chanData);
        synchronized (newChanList) {
            newChanList.add(chanData);
            if (selector != null) {
                selector.wakeup();
            }
        }
if(DEBUG_ADD)System.err.println("AddChanDone");

if(DEBUG_ADD)System.err.println("AddChanEnd");
        return chanData;
    }

    private void addNewChannels(Selector sel)
    {
final boolean DEBUG_NEW = false;
if(DEBUG_NEW)System.err.println("ANtop");
        synchronized (newChanList) {
            for (InputChannel cd : newChanList) {
if(DEBUG_NEW)System.err.println("ANreg "+cd);
                try {
                    cd.register(sel);
                } catch (ClosedChannelException cce) {
                    LOG.error("Cannot register closed channel", cce);
                    continue;
                }

if(DEBUG_NEW)System.err.println("ANadd "+cd);
                chanList.add(cd);

                if (isRunning()) {
                    cd.startReading();
                }
            }
            newChanList.clear();
            newChanList.notify();
        }
if(DEBUG_NEW)System.err.println("ANend");
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

    private void addSocketChannel(SocketChannel chan)
        throws IOException
    {
        addSocketChannel(chan, serverCache);
    }

    private InputChannel addSocketChannel(SocketChannel chan,
                                          IByteBufferCache bufCache)
        throws IOException
    {
        // disable blocking or receive engine will die
        chan.configureBlocking(false);
        return addDataChannel(chan, bufCache, bufferSize);
    }

    public void channelStopped()
    {
        channelStopFlag.set();
        if (selector != null) {
            selector.wakeup();
        }
    }

    public abstract InputChannel createChannel(SelectableChannel channel,
                                               IByteBufferCache bufMgr,
                                               int bufSize)
        throws IOException;

    public void destroyProcessor()
    {
        thread = null;

        setState(RunState.DESTROYED);
    }

    public void forcedStopProcessing()
    {
        setState(RunState.IDLE);

        notifyClient();
    }

    public synchronized Boolean[] getAllocationStopped() {
        ArrayList allocationStatus = new ArrayList();
        for (InputChannel cd : chanList) {
            Boolean bVal =
                (cd.isAllocationStopped() ? Boolean.TRUE : Boolean.FALSE);
            allocationStatus.add(bVal);
        }
        return (Boolean[]) allocationStatus.toArray(new Boolean[0]);
    }

    public synchronized Long[] getBufferCurrentAcquiredBuffers() {
        ArrayList byteLimit = new ArrayList();
        for (InputChannel cd : chanList) {
            byteLimit.add(new Long(cd.getBufferCurrentAcquiredBuffers()));
        }

        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getBufferCurrentAcquiredBytes() {
        ArrayList byteLimit = new ArrayList();
        for (InputChannel cd : chanList) {
            byteLimit.add(new Long(cd.getBufferCurrentAcquiredBytes()));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getBytesReceived() {
        ArrayList byteCount = new ArrayList();
        for (InputChannel cd : chanList) {
            byteCount.add(new Long(cd.getBytesReceived()));
        }
        return (Long[]) byteCount.toArray(new Long[0]);
    }

    public synchronized Long[] getLimitToRestartAllocation() {
        ArrayList byteLimit = new ArrayList();
        for (InputChannel cd : chanList) {
            byteLimit.add(new Long(cd.getLimitToRestartAllocation()));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getLimitToStopAllocation() {
        ArrayList byteLimit = new ArrayList();
        for (InputChannel cd : chanList) {
            byteLimit.add(new Long(cd.getLimitToStopAllocation()));
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public String getPresentState()
    {
        return state.toString();
    }

    public synchronized Long[] getRecordsReceived() {
        ArrayList recordCount = new ArrayList();
        for (InputChannel cd : chanList) {
            recordCount.add(new Long(cd.getRecordsReceived()));
        }
        return (Long[]) recordCount.toArray(new Long[0]);
    }

    public int getServerPort() {
        return port;
    }

    public synchronized Long[] getStopMessagesReceived() {
        ArrayList recordCount = new ArrayList();
        for (InputChannel cd : chanList) {
            recordCount.add(new Long(cd.getStopMessagesReceived()));
        }
        return (Long[]) recordCount.toArray(new Long[0]);
    }

    public synchronized long getTotalRecordsReceived() {
        long total = 0;
        for (InputChannel cd : chanList) {
            total += cd.getRecordsReceived();
        }
        return total;
    }

    public boolean isDestroyed()
    {
        return state == RunState.DESTROYED;
    }

    public boolean isDisposing()
    {
        return state == RunState.DISPOSING;
    }

    public boolean isError()
    {
        return state == RunState.ERROR;
    }

    public boolean isRunning()
    {
        return state == RunState.RUNNING;
    }

    public boolean isStopped()
    {
        return state == RunState.IDLE;
    }

    List<InputChannel> listChannels()
    {
        List<InputChannel> list = new ArrayList<InputChannel>();
        list.addAll(chanList);
        return list;
    }

    void makeReverseConnections()
        throws IOException
    {
        final int MAX_RETRIES = 10;

        if (!madeReverseConnections) {
            ArrayList<ReverseConnection> retries =
                new ArrayList<ReverseConnection>(reverseConnList);
            for (int i = 1; i <= MAX_RETRIES; i++) {
                boolean failed = false;

                Iterator<ReverseConnection> iter = retries.iterator();
                while (iter.hasNext()) {
                    ReverseConnection rc = iter.next();

                    try {
                        rc.connect();
                        iter.remove();
                    } catch (IOException ioe) {
                        if (i == MAX_RETRIES) {
                            LOG.error("Could not connect " + rc, ioe);
                        }
                        failed = true;
                    }
                }

                if (failed) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        // ignore interrupts
                    }
                }
            }
            madeReverseConnections = true;
        }

        // wait for new connections to be noticed
        synchronized (newChanList) {
            while (newChanList.size() > 0) {
                try {
                    newChanList.wait();
                } catch (InterruptedException ie) {
                    // ignore interrupts
                }
            }
        }
    }

    void notifyClient()
    {
        if (compObserver != null) {
            compObserver.update(NormalState.STOPPED, DAQCmdInterface.SINK);
        }
    }

    public void registerComponentObserver(DAQComponentObserver compObserver)
    {
        this.compObserver = compObserver;
    }

    public void run()
    {
        newState = RunState.IDLE;

final boolean DEBUG_RUN = false;
if(DEBUG_RUN)System.err.println("R-2");

        try {
            selector = Selector.open();
        } catch (IOException ioe) {
            throw new Error("Cannot create selector", ioe);
        }
if(DEBUG_RUN)System.err.println("R-1");

        while (thread != null) {
if(DEBUG_RUN)System.err.println("Rtop "+state+" new "+newState);

            if (newState != state) {
if(DEBUG_RUN)System.err.println("Rstate "+state+"->"+newState);

                switch (newState) {
                case RUNNING:
                    for (InputChannel cd : chanList) {
if(DEBUG_RUN)System.err.println("Rstart "+cd);
                        cd.startReading();
                    }
                    break;
                case IDLE:
                    // do nothing
                    break;
                case DISPOSING:
                    // do nothing
                    break;
                case DESTROYED:
                    break;
                default:
                    try {
                        throw new Error("Not handling state " + newState);
                    } catch (Error err) {
                        LOG.error("State change!", err);
                    }
                }

                state = newState;
            }

            // if startServer() wants thread to pause...
            if (pauseThread.isSet()) {
if(DEBUG_RUN)System.err.println("Rpause");

                synchronized (pauseThread) {
                    // notify startServer() that we've paused
                    pauseThread.notify();

                    // wait for startServer() to register server socket
                    try {
if(DEBUG_RUN)System.err.println("RpauseWait");
                        pauseThread.wait();
                    } catch (Exception ex) {
                        LOG.error("Thread pause was interrupted", ex);
                    }
                }

                if (pauseThread.isSet()) {
                    LOG.error("'pauseThread' set to true after pause");
                }
if(DEBUG_RUN)System.err.println("RpauseAwake");
            }

            int numSelected;
            try {
if(DEBUG_RUN)System.err.println("Rsel");
                numSelected = selector.select(SELECTOR_TIMEOUT);
            } catch (IOException ioe) {
                LOG.error("Error on selection: ", ioe);
                numSelected = 0;
if(DEBUG_RUN)System.err.println("Rselerr");
            }
if(DEBUG_RUN)System.err.println("Rnumsel="+numSelected);

            if (newChanList.size() > 0) {
if(DEBUG_RUN)System.err.println("Radd");
                addNewChannels(selector);
            }

            if (numSelected != 0) {
                Iterator iter =  selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey selKey = (SelectionKey) iter.next();
                    iter.remove();

                    // if this is a new connection to the input server...
                    if (selKey.isAcceptable()) {
if(DEBUG_RUN)System.err.println("Raccept " + state);
                        ServerSocketChannel ssChan =
                            (ServerSocketChannel) selKey.channel();

                        try {
                            SocketChannel chan = ssChan.accept();
if(DEBUG_RUN)System.err.println("RsockChan "+chan);

                            // if server channel is non-blocking,
                            // chan may be null

                            if (chan != null) {
                                addSocketChannel(chan);
if(DEBUG_RUN)System.err.println("Radded");
                            }

if(DEBUG_RUN)System.err.println("New chan is"+(chan.isRegistered()?"":" NOT")+" registered");
                        } catch (IOException ioe) {
                            LOG.error("Couldn't accept client socket", ioe);
                        }
                        continue;
                    }

                    if (state != RunState.RUNNING &&
                        state != RunState.DISPOSING)
                    {
if(DEBUG_RUN)System.err.println("Rcancel "+state);

                        selKey.cancel();
                    } else {
                        InputChannel chanData =
                            (InputChannel) selKey.attachment();
                        try {
if(DEBUG_RUN)System.err.println("Rproc chanData "+chanData);
                            chanData.processSelect(selKey);
                        } catch (ClosedChannelException cce) {
                            // channel went away
                            selKey.cancel();
                            chanList.remove(chanData);
                            if (chanList.size() > 0) {
                                LOG.error("Closed " + name +
                                          " socket channel, " +
                                          chanList.size() +
                                          " channels remain");
                            } else {
                                LOG.error("Closed " + name +
                                          " socket channel, stopping reader");
                                channelStopFlag.set();
                            }
                        } catch (IOException ioe) {
                            LOG.error("Could not process select", ioe);
                        }
                    }
                }
            }

            if (channelStopFlag.isSet()) {
                channelStopFlag.clear();
if(DEBUG_RUN)System.err.println("RchkStop "+chanList.size()+" chans "+chanList);

                boolean running = false;
                for (Iterator<InputChannel> iter = chanList.iterator();
                     iter.hasNext(); )
                {
                    InputChannel chanData = iter.next();

                    if (chanData.isStopped()) {
if(DEBUG_RUN)System.err.println("Rkill "+chanData);
                        try {
                            chanData.close();
                        } catch (IOException ioe) {
                            LOG.error("Couldn't close input channel " +
                                      chanData, ioe);
                        }

                        iter.remove();
                    } else {
if(DEBUG_RUN)System.err.println("RchanRun "+chanData);
                        running = true;
                        break;
                    }
                }

                if (!running) {
if(DEBUG_RUN)System.err.println("RnotRun");

                    synchronized (stateLock) {
                        newState = RunState.IDLE;
                    }
                    madeReverseConnections = false;
                    notifyClient();
if(DEBUG_RUN)System.err.println("Ridle");
                }
            }
if(DEBUG_RUN)System.err.println("Rbottom");
        }

        state = RunState.DESTROYED;
if(DEBUG_RUN)System.err.println("Rexit");
    }

    private void setState(RunState newState)
    {
final boolean DEBUG_SET = false;
if(DEBUG_SET)System.err.println("SSTtop");
        synchronized (stateLock) {
            this.newState = newState;
if(DEBUG_SET)System.err.println("SSTnewState="+newState);
            stateLock.notify();
            if (selector != null) {
                selector.wakeup();
            }
        }
if(DEBUG_SET)System.err.println("SSTend");
    }

    public void start()
    {
        if (thread != null) {
            throw new Error("Thread is already running");
        }

        thread = new Thread(this);
        thread.setName(name + "Thread");

        thread.start();
    }

    public void startDisposing()
    {
        synchronized (stateLock) {
            if (state != RunState.RUNNING) {
                throw new Error("Cannot start disposing " + state +
                                " payload reader");
            }

            setState(RunState.DISPOSING);
        }
    }

    public void startProcessing()
    {
        try {
            makeReverseConnections();
        } catch (IOException ioe) {
            throw new RuntimeException("Cannot make reverse connections", ioe);
        }

        synchronized (stateLock) {
            if (state != RunState.IDLE) {
                throw new Error("Cannot start " + state + " payload reader");
            }

            setState(RunState.RUNNING);
        }
    }

    public void startServer(IByteBufferCache serverCache)
        throws IOException
    {
final boolean DEBUG_SS = false;
if(DEBUG_SS)System.err.println("SStop");
        // hack around Linux kernel bug:
        //     ssChan.register() blocks for an indefinite amount of time if
        //     called while a select is in progress, so we pause the thread,
        //     register the server socket, and then resume the thread
        //
        synchronized (pauseThread) {
if(DEBUG_SS)System.err.println("SSinBlk");
            if (pauseThread.isSet()) {
                LOG.error("Thread is already paused -- this could be bad!");
            }

if(DEBUG_SS)System.err.println("SSsetFlag");
            // indicate to worker thread that it should pause
            pauseThread.set();

            // make sure worker thread isn't stuck someplace
if(DEBUG_SS)System.err.println("SSnotify");
            synchronized (stateLock) {
                stateLock.notify();
                if (selector != null) {
                    selector.wakeup();
                }
            }

            // wait for thread to notify us that it has paused
            try {
if(DEBUG_SS)System.err.println("SSwait");
                pauseThread.wait();
            } catch (Exception ex) {
                LOG.error("Couldn't wait for pauseThread", ex);
            }
        }
if(DEBUG_SS)System.err.println("SSwork");

        ServerSocketChannel ssChan = ServerSocketChannel.open();
        ssChan.configureBlocking(false);

        ssChan.socket().bind(null);
        port = ssChan.socket().getLocalPort();

        ssChan.register(selector, SelectionKey.OP_ACCEPT);

if(DEBUG_SS)System.err.println("SSready");
        synchronized (pauseThread) {
            // turn off pause flag
            if (!pauseThread.isSet()) {
if(DEBUG_SS)System.err.println("SSerr");
                LOG.error("Expected thread to be paused!");
            } else {
if(DEBUG_SS)System.err.println("SSclr");
                pauseThread.clear();
            }

            // let the thread know that we're done
if(DEBUG_SS)System.err.println("SSrenotify");
            pauseThread.notify();
        }
if(DEBUG_SS)System.err.println("SSdone");

        this.serverCache = serverCache;
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

            boolean finished = false;
            for (int i = 0; i < 10; i++) {
                if (sock.finishConnect()) {
                    finished = true;
                    break;
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    // ignore interrupts
                }
            }

            if (!finished) {
                throw new IOException("Could not finish connection to " +
                                      hostName + ":" + port);
            }

            addSocketChannel(sock, bufCache);
        }

        public String toString()
        {
            return hostName + ":" + port;
        }
    }
}
