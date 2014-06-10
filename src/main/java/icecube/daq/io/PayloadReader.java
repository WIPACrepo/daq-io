package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
    implements DAQComponentInputProcessor, IOChannelParent, Runnable
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
    private Selector selector;
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
    private Flag pauseThread = new Flag("pauseThread");
    // server byte buffer
    private IByteBufferCache serverCache;

    // reverse connection list
    private ArrayList<ReverseConnection> reverseConnList =
        new ArrayList<ReverseConnection>();
    // <tt>true</tt> if reverse connections have been made
    private boolean madeReverseConnections;

    // channel on which server listens for connections
    private ServerSocketChannel serverChannel;

    // total records received by channels which have been removed
    private int totalReceivedFromRemovedChannels;

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

    public InputChannel addDataChannel(SelectableChannel channel, String name,
                                       IByteBufferCache bufMgr)
        throws IOException
    {
        return addDataChannel(channel, name, bufMgr, bufferSize);
    }

    public InputChannel addDataChannel(SelectableChannel channel, String name,
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
        InputChannel chanData = createChannel(channel, name, bufMgr, bufSize);
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
                synchronized (chanList) {
                    chanList.add(cd);
                }

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
        return addDataChannel(chan, name, bufCache, bufferSize);
    }

    /**
     * Channel encountered an error.
     *
     * @param chan channel
     * @param buf ByteBuffer which caused the error (may ne <tt>null</tt>)
     * @ex exception (may be null)
     */
    public void channelError(IOChannel chan, ByteBuffer buf,
                             Exception ex)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Channel has stopped.
     *
     * @param chan channel
     */
    public void channelStopped(IOChannel chan)
    {
        channelStopFlag.set();
        if (selector != null) {
            selector.wakeup();
        }
    }

    public abstract InputChannel createChannel(SelectableChannel channel,
                                               String name,
                                               IByteBufferCache bufMgr,
                                               int bufSize)
        throws IOException;

    public void destroyProcessor()
    {
        thread = null;

        if (serverChannel != null) {
            try {
                serverChannel.close();
            } catch (IOException ioe) {
                LOG.error("Cannot close " + name + " server channel", ioe);
            }

            serverChannel = null;
        }

        setState(RunState.DESTROYED);
    }

    public void forcedStopProcessing()
    {
        setState(RunState.IDLE);

        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                cd.notifyOnStop();
            }
        }

        notifyClient();
    }

    public synchronized Boolean[] getAllocationStopped() {
        ArrayList allocationStatus = new ArrayList();
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                Boolean bVal =
                    (cd.isAllocationStopped() ? Boolean.TRUE : Boolean.FALSE);
                allocationStatus.add(bVal);
            }
        }
        return (Boolean[]) allocationStatus.toArray(new Boolean[0]);
    }

    public synchronized Long[] getBufferCurrentAcquiredBuffers() {
        ArrayList byteLimit = new ArrayList();
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                byteLimit.add(new Long(cd.getBufferCurrentAcquiredBuffers()));
            }
        }

        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getBufferCurrentAcquiredBytes() {
        ArrayList byteLimit = new ArrayList();
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                byteLimit.add(new Long(cd.getBufferCurrentAcquiredBytes()));
            }
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getBytesReceived() {
        ArrayList byteCount = new ArrayList();
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                byteCount.add(new Long(cd.getBytesReceived()));
            }
        }
        return (Long[]) byteCount.toArray(new Long[0]);
    }

    public synchronized Long[] getLimitToRestartAllocation() {
        ArrayList byteLimit = new ArrayList();
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                byteLimit.add(new Long(cd.getLimitToRestartAllocation()));
            }
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    public synchronized Long[] getLimitToStopAllocation() {
        ArrayList byteLimit = new ArrayList();
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                byteLimit.add(new Long(cd.getLimitToStopAllocation()));
            }
        }
        return (Long[]) byteLimit.toArray(new Long[0]);
    }

    /**
     * Return reader name
     *
     * @return name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Return number of active channels.
     *
     * @return number of active channels
     */
    public int getNumberOfChannels()
    {
        return chanList.size();
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
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                recordCount.add(new Long(cd.getStopMessagesReceived()));
            }
        }
        return (Long[]) recordCount.toArray(new Long[0]);
    }

    public String getStringExtra()
    {
        return "";
    }

    public synchronized long getTotalRecordsReceived() {
        long total = totalReceivedFromRemovedChannels;
        synchronized (chanList) {
            for (InputChannel cd : chanList) {
                total += cd.getRecordsReceived();
            }
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

    public boolean isPaused()
    {
        synchronized (pauseThread) {
            return pauseThread.isSet();
        }
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
        synchronized (chanList) {
            list.addAll(chanList);
        }
        return list;
    }

    void makeReverseConnections()
        throws IOException
    {
        if (!madeReverseConnections) {
            final int MAX_RETRIES = 10;

            ArrayList<ReverseConnection> retries =
                new ArrayList<ReverseConnection>(reverseConnList);

            int numRetries = 0;
            IOException ex = null;

            for ( ; numRetries < MAX_RETRIES; numRetries++) {
                Iterator<ReverseConnection> iter = retries.iterator();
                while (iter.hasNext()) {
                    ReverseConnection rc = iter.next();

                    try {
                        rc.connect();
                        iter.remove();
                    } catch (IOException ioe) {
                        ex = ioe;
                    }
                }

                if (retries.size() > 0) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ie) {
                        // ignore interrupts
                    }
                }
            }
            madeReverseConnections = true;

            if (retries.size() > 0) {
                LOG.error("Could not connect " + retries.get(0), ex);
            } else {
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
        }
    }

    void notifyClient()
    {
        if (compObserver != null) {
            compObserver.update(NormalState.STOPPED, DAQCmdInterface.SINK);
        }
    }


    public void pause()
    {
        synchronized (pauseThread) {
            if (pauseThread.isSet()) {
                LOG.error("Thread is already paused!");
                return;
            }

            // indicate to worker thread that it should pause
            pauseThread.set();

            // make sure worker thread isn't stuck someplace
            synchronized (stateLock) {
                stateLock.notify();
                if (selector != null) {
                    selector.wakeup();
                }
            }

            // wait for thread to notify us that it has paused
            try {
                pauseThread.wait();
            } catch (Exception ex) {
                LOG.error("Couldn't wait for pauseThread", ex);
            }
        }
    }

    public void registerComponentObserver(DAQComponentObserver compObserver)
    {
        this.compObserver = compObserver;
    }

    private void removeChannel(InputChannel chanData)
    {
        synchronized (chanList) {
            chanList.remove(chanData);
            if (chanList.size() > 0) {
                LOG.error("Closed " + name + " socket channel, " +
                          chanList.size() + " channels remain");
            } else {
                LOG.error("Closed " + name +
                          " socket channel, stopping reader");
                channelStopFlag.set();
            }
        }
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
                    synchronized (chanList) {
                        for (InputChannel cd : chanList) {
if(DEBUG_RUN)System.err.println("Rstart "+cd);
                            cd.startReading();
                        }
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

                    InputChannel chanData = (InputChannel) selKey.attachment();

                    if (state != RunState.RUNNING &&
                        state != RunState.DISPOSING)
                    {
if(DEBUG_RUN)System.err.println("Rremove "+state);
                        if (!chanData.isOpen()) {
                            try {
                                chanData.close();
                            } catch (Exception ex) {
                                LOG.error("Cannot close closed channel", ex);
                            }
                        }
                        // XXX should we close noisy channels?
                        removeChannel(chanData);
                        selKey.cancel();
                    } else {
                        try {
if(DEBUG_RUN)System.err.println("Rproc chanData "+chanData);
                            chanData.processSelect(selKey);
                        } catch (ClosedChannelException cce) {
                            // channel went away
                            selKey.cancel();
                            removeChannel(chanData);
                        } catch (IOException ioe) {
                            selKey.cancel();
                            removeChannel(chanData);
                        }
                    }
                }
            }

            if (channelStopFlag.isSet()) {
                channelStopFlag.clear();
if(DEBUG_RUN)System.err.println("RchkStop "+chanList.size()+" chans "+chanList);

                boolean running = false;
                synchronized (chanList) {
                    for (Iterator<InputChannel> iter = chanList.iterator();
                         iter.hasNext(); )
                    {
                        InputChannel chanData = iter.next();

                        if (!chanData.isStopped()) {
if(DEBUG_RUN)System.err.println("RchanRun "+chanData);
                            running = true;
                        } else {
if(DEBUG_RUN)System.err.println("Rkill "+chanData);
                            totalReceivedFromRemovedChannels +=
                                chanData.getRecordsReceived();
                            try {
                                chanData.close();
                            } catch (IOException ioe) {
                                LOG.error("Couldn't close input channel " +
                                          chanData, ioe);
                            }

                            iter.remove();
                        }
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

        try {
            selector.close();
            selector = null;
        } catch (IOException ioe) {
            // ignore errors
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
        if (reverseConnList.size() > 0) {
            try {
                makeReverseConnections();
            } catch (IOException ioe) {
                throw new RuntimeException("Cannot make reverse connections",
                                           ioe);
            }
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
if(DEBUG_SS)System.err.println("SSpause");
        pause();
if(DEBUG_SS)System.err.println("SSwork");

        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);

        serverChannel.socket().bind(null);
        port = serverChannel.socket().getLocalPort();

        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

if(DEBUG_SS)System.err.println("SSready");
        unpause();
if(DEBUG_SS)System.err.println("SSdone");

        this.serverCache = serverCache;
    }

    public void unpause()
    {
        synchronized (pauseThread) {
            // turn off pause flag
            if (!pauseThread.isSet()) {
                LOG.error("Expected thread to be paused!");
            } else {
                pauseThread.clear();
            }

            // let the thread know that we're done
            pauseThread.notify();
        }
    }

    public String toString()
    {
        return name + "[" + state + "," + chanList.size() + " chan," +
            getTotalRecordsReceived() + " sent" + getStringExtra() + "]";
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
