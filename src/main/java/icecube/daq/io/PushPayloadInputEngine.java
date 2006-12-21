package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedBoolean;
import EDU.oswego.cs.dl.util.concurrent.WaitableBoolean;

import icecube.daq.common.DAQComponentObserver;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.SourceIdRegistry;

import icecube.daq.payload.impl.SourceID4B;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.ReadableByteChannel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Input engine which pushes payloads to its client.
 */
public abstract class PushPayloadInputEngine
    extends PayloadInputEngine
{
    private static final int WAIT_FOR_INPUT_MSEC = 200;

    private static final Log LOG =
        LogFactory.getLog(PushPayloadInputEngine.class);

    private IByteBufferCache bufMgr;

    private ArrayList receiveList = new ArrayList();
    private WaitableBoolean startProcessing = new WaitableBoolean(false);
    private SynchronizedBoolean disposeFlag = new SynchronizedBoolean(false);

    private boolean destroyed;
    private boolean lastMessageReceived;

    private Thread inputThread;

    // data accessed by mbean interface
    private long stopMessagesPropagated;
    private long dequeuedMessages;
    private long dequeueErrors;

    // monitoring data
    private long totStops;

    /**
     * Create an input engine which pushes payloads to its client.
     *
     * @param name DAQ component name
     * @param id DAQ component ID
     * @param fcn engine function
     * @param prefix identifying prefix for engine thread name
     * @param bufMgr byte buffer cache manager
     */
    public PushPayloadInputEngine(String name, int id, String fcn,
                                  String prefix, IByteBufferCache bufMgr)
    {
        // parent constructor wants same args
        super(name, id, fcn);

        if (bufMgr == null) {
            throw new IllegalArgumentException("Buffer cache manager cannot" +
                                               " be null");
        }
        this.bufMgr = bufMgr;

        // name the thread for thread dumps
        inputThread = new Thread(new InputEngineThread());
        inputThread.setName(prefix + "InputEngine-" + name +
                            "-id#" + id + "-" + fcn);
    }

    /**
     * Add an input channel to this engine.
     *
     * @param channel channel to add
     * @param bufMgr byte buffer cache
     */
    public PayloadReceiveChannel addDataChannel(ReadableByteChannel channel,
                                                IByteBufferCache bufMgr)
    {
        if (!isStopped()) {
            final String errMsg = "Cannot add data channel to running engine";
            throw new RuntimeException(errMsg);
        }

        // add the channel to base class
        PayloadReceiveChannel rcvEngine =
            super.addDataChannel(channel, bufMgr);

        receiveList.add(rcvEngine);

        return rcvEngine;
    }

    /**
     * Stop worker threads.
     *
     * @throws Exception if there is a problem
     */
    public void destroyProcessor()
    {
        super.destroyProcessor();
        destroyed = true;
        // make sure thread will run
        startProcessing.set(true);
    }

    /**
     * Get the byte buffer cache associated with this input engine.
     *
     * @return byte buffer cache
     */
    IByteBufferCache getByteBufferCache()
    {
        return bufMgr;
    }

    /**
     * Get the number of erroneous messages received during this run.
     *
     * @return number of erroneous messages received
     */
    public long getDequeueErrors()
    {
        return dequeueErrors;
    }

    /**
     * Get the number of messages received during this run.
     *
     * @return number of messages received
     */
    public long getDequeuedMessages()
    {
        return dequeuedMessages;
    }

    /**
     * Get the number of stop messages passed on to other objects
     * during this run.
     *
     * @return total number of stop messages
     */
    public long getStopMessagesPropagated()
    {
        return stopMessagesPropagated;
    }

    /**
     * Get the total number of stop messages received.
     *
     * @return total number of stop messages
     */
    public long getTotalStopsReceived()
    {
        return totStops;
    }

    protected void notifyClient()
    {
        super.notifyClient();

        // check if we are disposing
        if (disposeFlag.get()) {
            disposeFlag.set(false);
        } else {
            // must be a normal stop, let the thread notify
            // parent when its done.
            totStops++;

            // notify the worker thread that it should stop
            lastMessageReceived = true;
        }
    }

    public abstract void pushBuffer(ByteBuffer bb)
        throws IOException;

    public abstract void sendStop();

    /**
     * Start worker threads.
     */
    public void start()
    {
        // first start the payload classes
        super.start();
        // now start our thread
        inputThread.start();
    }

    /**
     * Start disposing input.
     */
    public void startDisposing()
    {
        disposeFlag.set(true);
        super.startDisposing();
    }

    /**
     * Start processing input.
     */
    public void startProcessing()
    {
        super.startProcessing();
        if (super.isRunning()) {
            startProcessing.set(true);
        }
    }

    /**
     * Worker thread.
     */
    private class InputEngineThread
        implements Runnable
    {
        public void run()
        {
            boolean inputIsAvailable;

            while (true) {
                try {
                    startProcessing.whenTrue(null);
                    startProcessing.set(false);
                    lastMessageReceived = false;
                    // clear out some counters at start up
                    dequeuedMessages = 0;
                    dequeueErrors = 0;
                    stopMessagesPropagated = 0;
                } catch (InterruptedException ie) {
                    LOG.error("problem here: ", ie);
                }

                // now start the real work
                while (true) {

                    // look for any input data...wait a bit
                    try {
                        inputIsAvailable =
                            inputAvailable.attempt(WAIT_FOR_INPUT_MSEC);
                    } catch (InterruptedException ie) {
                        LOG.error("problem here: ", ie);
                        inputIsAvailable = false;
                    }

                    // should we exit?
                    if (destroyed) {
                        // we should be well out of here before a destroy comes
                        // through synchronize clean up to make sure other
                        // threads don't get surprised when we are no longer
                        // here.
                        return;
                    }

                    // are we disposing?
                    if (disposeFlag.get()) {
                        disposeFlag.set(false);
                        break;
                    }

                    if (inputIsAvailable) {
                        // check for new data in input queues
                        Iterator receiveIter =
                            receiveList.iterator();
                        while (receiveIter.hasNext()) {
                            PayloadReceiveChannel payloadEngine =
                                (PayloadReceiveChannel) receiveIter.next();

                            // see if any engine has data
                            while (!payloadEngine.inputQueue.isEmpty()) {
                                // get input message
                                try {
                                    ByteBuffer buf =
                                        (ByteBuffer)
                                        payloadEngine.inputQueue.take();
                                    pushBuffer(buf);
                                    // count them
                                    dequeuedMessages++;
                                } catch (Exception e) {
                                    // count errors
                                    dequeueErrors++;
                                    if (LOG.isErrorEnabled()) {
                                        LOG.error("Unpacking error#" +
                                                 dequeueErrors, e);
                                    }
                                }
                            }
                            // take a semaphore to keep things even; but
                            // treat the first one differently
                            //
                            // ***NOTE:  returning too many permits via
                            // attempt(0) can cause this to block.  So, one
                            // (successful) call to attempt per message
                            // received.  See oswego concurrent
                            // web site for details on counting semaphores.
                            if (inputIsAvailable) {
                                inputIsAvailable = false;
                            } else {
                                try {
                                    inputAvailable.attempt(0);
                                } catch (InterruptedException ie) {
                                    LOG.error("problem here: ", ie);
                                }
                            }
                        }
                    }

                    if (lastMessageReceived) {
                        sendStop();
                        stopMessagesPropagated++;
                        // break out to outer loop
                        break;
                    }
                }
            }
        }
    }
}
