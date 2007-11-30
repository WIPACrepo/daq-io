package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;

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

public abstract class SimpleReader
    implements DAQComponentInputProcessor, IOChannelParent, Runnable
{
    /** default input buffer size */
    static final int DEFAULT_BUFFER_SIZE = 2048;

    /** logging object */
    private static final Log LOG = LogFactory.getLog(SimpleReader.class);

    /** selector timeout (in msec.) */
    private static final int SELECTOR_TIMEOUT = 1000;

    /** run states */
    private enum State {
        CREATED, IDLE, RUNNING, DISPOSING, DESTROYED, ERROR
    }

    /** reader name */
    private String name;
    /** input buffer size */
    private int bufferSize;

    /** current state */
    private State state;

    /** external observer */
    private DAQComponentObserver observer;

    /** Numeric ID used for next added channel */ 
    private int nextChannelNum;
    /** List of active channels */
    private ArrayList<SimpleChannel> channelList =
        new ArrayList<SimpleChannel>();

    /** worker thread */
    private Thread thread;
    /** dynamically assigned server port number */
    private int port = Integer.MIN_VALUE;
    /** server byte buffer */
    private IByteBufferCache serverCache;
    /** Has the server thread started? */
    private boolean serverStarted;

    /** List of reverse connections */
    private ArrayList<ReverseConnection> reverseConnList =
        new ArrayList<ReverseConnection>();
    /** Have the reverse connections been made? */
    private boolean madeReverseConnections;

    public SimpleReader(String name)
    {
        this(name, DEFAULT_BUFFER_SIZE);
    }

    public SimpleReader(String name, int bufferSize)
    {
        this.name = name;
        this.bufferSize = bufferSize;

        state = State.CREATED;
    }

    public SimpleChannel addDataChannel(SelectableChannel channel,
                                        IByteBufferCache bufMgr)
        throws IOException
    {
        return addDataChannel(channel, bufMgr, bufferSize);
    }

    public SimpleChannel addDataChannel(SelectableChannel channel,
                                        IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        if (state != State.IDLE) {
            final String errMsg = "Cannot add data channel while engine is " +
                getPresentState();
            throw new Error(errMsg);
        }

        channel.configureBlocking(false);

        int chanNum = nextChannelNum++;

        String name = "SimpleReader#" + chanNum;

        SimpleChannel chanData = createChannel(name, channel, bufMgr, bufSize);
        synchronized (channelList) {
            channelList.add(chanData);
        }

        return chanData;
    }

    public void addReverseConnection(String hostName, int port,
                                     IByteBufferCache bufCache)
        throws IOException
    {
        if (state != State.IDLE) {
            final String errMsg = "Cannot add reverse connection" +
                " while engine is " + getPresentState();
            throw new Error(errMsg);
        }

        synchronized (reverseConnList) {
            ReverseConnection rConn =
                new ReverseConnection(hostName, port, bufCache);
            reverseConnList.add(rConn);
        }
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
        LOG.error("Channel " + chan + (buf == null ? "" : " buf " + buf), ex);

        state = State.ERROR;
    }

    /**
     * Channel has stopped.
     *
     * @param chan channel
     */
    public void channelStopped(IOChannel chan)
    {
        synchronized (channelList) {
            if (channelList.size() > 0) {
                channelList.remove(chan);

                if (channelList.size() == 0) {
                    if (observer != null) {
                        observer.update(NormalState.STOPPED,
                                        DAQCmdInterface.SINK);
                    }

                    state = State.IDLE;
                    madeReverseConnections = false;
                }
            }
        }
    }

    public abstract SimpleChannel createChannel(String name,
                                                SelectableChannel channel,
                                                IByteBufferCache bufMgr,
                                                int bufSize)
        throws IOException;

    public void destroyProcessor()
    {
        synchronized (channelList) {
            for (SimpleChannel chan :
                 new ArrayList<SimpleChannel>(channelList))
            {
                chan.stopProcessing();
            }

            state = State.DESTROYED;
        }

        thread = null;
    }

    public void forcedStopProcessing()
    {
        if (state != State.RUNNING && state != State.DISPOSING) {
            final String errMsg = "Cannot add reverse connection" +
                " while engine is " + getPresentState();
            throw new Error(errMsg);
        }

        synchronized (channelList) {
            if (channelList.size() == 0) {
                state = State.IDLE;
                madeReverseConnections = false;
            } else {
                for (SimpleChannel chan :
                     new ArrayList<SimpleChannel>(channelList))
                {
                    chan.stopProcessing();
                }
            }
        }
    }

    public boolean[] getAllocationStopped() {
        boolean[] array;
        synchronized (channelList) {
            array = new boolean[channelList.size()];

            int idx = 0;
            for (SimpleChannel chan : channelList) {
                array[idx++] = chan.isAllocationStopped();
            }
        }
        return array;
    }

    public long[] getBytesReceived()
    {
        long[] array;
        synchronized (channelList) {
            array = new long[channelList.size()];

            int idx = 0;
            for (SimpleChannel chan : channelList) {
                array[idx++] = chan.getBytesReceived();
            }
        }
        return array;
    }

    public String getPresentState()
    {
        return state.toString();
    }

    public int getServerPort()
    {
        return port;
    }

    public long[] getStopMessagesReceived()
    {
        long[] array;
        synchronized (channelList) {
            array = new long[channelList.size()];
            int idx = 0;
            for (SimpleChannel chan : channelList) {
                array[idx++] = chan.getStopMessagesReceived();
            }
        }
        return array;
    }

    public long[] getRecordsReceived()
    {
        long[] array;
        synchronized (channelList) {
            array = new long[channelList.size()];
            int idx = 0;
            for (SimpleChannel chan : channelList) {
                array[idx++] = chan.getRecordsReceived();
            }
        }
        return array;
    }

    public long getTotalRecordsReceived() {
        long total = 0;
        synchronized (channelList) {
            int idx = 0;
            for (SimpleChannel chan : channelList) {
                total += chan.getRecordsReceived();
            }
        }
        return total;
    }

    public boolean isDestroyed()
    {
        return state == State.DESTROYED;
    }

    public boolean isDisposing()
    {
        return state == State.DISPOSING;
    }

    public boolean isError()
    {
        return state == State.ERROR;
    }

    public boolean isRunning()
    {
        return state == State.RUNNING;
    }

    public boolean isServerStarted()
    {
        return serverStarted;
    }

    public boolean isStopped()
    {
        return state == State.IDLE;
    }

    List<SimpleChannel> listChannels()
    {
        return new ArrayList<SimpleChannel>(channelList);
    }

    /**
     * Make reverse connections to remote servers.
     *
     * @throws IOException if a connection could not be made
     */
    void makeReverseConnections()
        throws IOException
    {
        if (!madeReverseConnections) {
            final int MAX_RETRIES = 10;

            final int numChan = channelList.size();

            ArrayList<ReverseConnection> retries =
                new ArrayList<ReverseConnection>(reverseConnList);

            int numRetries = 0;
            IOException ex = null;

            for ( ; numRetries < MAX_RETRIES; numRetries++) {
                boolean failed = false;

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
                for (int i = 0; i < 100; i++) {
                    if (channelList.size() == numChan) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ie) {
                            // ignore interrupts
                        }
                    }
                }
            }
        }
    }

    /**
     * Register an observer of this input engine.
     *
     * @param compObserver observer
     */
    public void registerComponentObserver(DAQComponentObserver compObserver)
    {
        this.observer = compObserver;
    }

    /**
     * Server thread which accepts connections from remote components.
     */
    public void run()
    {
        Selector selector;
        try {
            selector = Selector.open();

            ServerSocketChannel ssChan = ServerSocketChannel.open();
            ssChan.configureBlocking(false);

            ssChan.socket().bind(null);
            port = ssChan.socket().getLocalPort();

            ssChan.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException ioe) {
            thread = null;
            throw new Error("Cannot initialize server thread", ioe);
        }

        serverStarted = true;

        while (thread != null) {
            int numSelected;
            try {
                numSelected = selector.select(SELECTOR_TIMEOUT);
            } catch (IOException ioe) {
                LOG.error("Error on selection: ", ioe);
                numSelected = 0;
            }

            if (numSelected != 0) {
                Iterator iter =  selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = (SelectionKey) iter.next();
                    iter.remove();

                    // there should only be 'accept' notifications
                    if (!key.isAcceptable()) {
                        LOG.error("Cancelling unknown key #" +
                                  key.interestOps() + " (" +
                                  (key.isValid() ? "" : "!") + "valid " +
                                  (key.isAcceptable() ? "" : "!") + "accept " +
                                  (key.isConnectable() ? "" : "!") + "conn " +
                                  (key.isReadable() ? "" : "!") + "read " +
                                  (key.isWritable() ? "" : "!") + "write " +
                                  ")");
                        key.cancel();
                        continue;
                    }

                    // this must be a new connection to the input server...
                    ServerSocketChannel ssChan =
                        (ServerSocketChannel) key.channel();

                    try {
                        SocketChannel chan = ssChan.accept();

                        // if server channel is non-blocking,
                        // chan may be null

                        if (chan != null) {
                            addDataChannel(chan, serverCache, bufferSize);
                        }
                    } catch (IOException ioe) {
                        LOG.error("Couldn't accept client socket", ioe);
                    }
                }
            }
        }

        serverStarted = false;
    }

    /**
     * Start the reader.
     */
    public void start()
    {
        state = State.IDLE;
    }

    /**
     * Start disposing incoming payloads.
     */
    public void startDisposing()
    {
        if (state != State.RUNNING) {
            final String errMsg = "Cannot start disposing while engine is " +
                getPresentState();
            throw new Error(errMsg);
        }

        synchronized (channelList) {
            for (SimpleChannel chan : channelList) {
                chan.startDisposing();
            }
        }

        state = State.DISPOSING;
    }

    /**
     * Start processing data from the connected channels.
     */
    public void startProcessing()
    {
        if (state != State.IDLE) {
            final String errMsg = "Cannot start processing while engine is " +
                getPresentState();
            throw new Error(errMsg);
        }

        if (reverseConnList.size() > 0) {
            try {
                makeReverseConnections();
            } catch (IOException ioe) {
                throw new Error("Cannot make reverse connections", ioe);
            }
        }

        synchronized (channelList) {
            for (SimpleChannel chan : channelList) {
                chan.startProcessing();
            }
        }

        state = State.RUNNING;
    }

    /**
     * Start the server thread.
     *
     * @param bufMgr cache manager used payloads from connected sockets
     */
    public void startServer(IByteBufferCache bufMgr)
        throws IOException
    {
        if (thread != null) {
            throw new Error("Server thread is already running");
        }

        if (state != State.IDLE) {
            final String errMsg = "Cannot start server thread" +
                " while engine is " + getPresentState();
            throw new Error(errMsg);
        }

        serverCache = bufMgr;
 
        thread = new Thread(this);
        thread.setName(name);
        thread.start();
   }

    /**
     * An internet port which input engine needs to connect to in order to
     * receive data.
     */
    class ReverseConnection
    {
        /** remote machine name or IP address */
        private String hostName;
        /** remote port */
        private int port;
        /** buffer cache manager for incoming payloads */
        private IByteBufferCache bufCache;

        /**
         * Description of a reverse connection.
         *
         * @param hostName remote machine name/IP address
         * @param port remote port
         * @param bufCache buffer manager
         */
        ReverseConnection(String hostName, int port, IByteBufferCache bufCache)
        {
            this.hostName = hostName;
            this.port = port;
            this.bufCache = bufCache;
        }

        /**
         * Make the connection
         *
         * @throws IOException if the connection could not be made
         */
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

            addDataChannel(sock, bufCache, bufferSize);
        }

        /**
         * Return a description of this reverse connection.
         *
         * @return debugging string
         */
        public String toString()
        {
            return hostName + ":" + port;
        }
    }
}
