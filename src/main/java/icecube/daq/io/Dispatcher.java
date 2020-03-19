/*
 * interface: Dispatcher
 *
 * Version $Id: Dispatcher.java 17771 2020-03-19 22:06:07Z dglo $
 *
 * Date: April 1 2004
 *
 * (c) 2004 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * This interface specifies how events are dispatched from the DAQ system.
 *
 * @author patton
 * @version $Id: Dispatcher.java 17771 2020-03-19 22:06:07Z dglo $
 */
public interface Dispatcher
{
    String START_PREFIX = "RunStart:";
    String STOP_PREFIX = "RunStop:";
    String SUBRUN_START_PREFIX = "SubrunStart:";
    String CLOSE_PREFIX = "Close:";
    String SWITCH_PREFIX = "Switch:";

    /**
     * Close current file (if open)
     *
     * @throws DispatchException if there is a problem
     */
    void close()
        throws DispatchException;

    /**
     * Signals to the dispatch system that the set of events that preced this
     * call are separated, by some criteria, for those that succeed it.
     *
     * @throws DispatchException if there is a problem in the Dispatch system.
     */
    void dataBoundary()
        throws DispatchException;

    /**
     * Signals to the dispatch system that the set of events that preced this
     * call are separated, by some criteria, for those that succeed it.
     * <p>
     * The message supplied with this method is opaque to the system, i.e. it
     * is not used by the system, and it simple passed on through the any
     * delivery client.
     *
     * @param message a String explaining the reason for the boundary.
     * @throws DispatchException if there is a problem in the Dispatch system.
     */
    void dataBoundary(String message)
        throws DispatchException;

    /**
     * Copies the event in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     *
     * @param buffer the ByteBuffer containg the event.
     * @param ticks DAQ time for this payload
     *
     * @throws DispatchException if there is a problem in the Dispatch system.
     */
    void dispatchEvent(ByteBuffer buffer, long ticks)
        throws DispatchException;

    /**
     * Dispatch a Payload event object
     *
     * @param event A payload object.
     * @throws DispatchException if there is a problem in the Dispatch system.
     */
    void dispatchEvent(IPayload event)
        throws DispatchException;

    /**
     * Get the byte buffer cache being used.
     *
     * @return byte buffer cache
     */
    IByteBufferCache getByteBufferCache();

    /**
     * Get the destination directory where the dispatch files will be saved.
     *
     * @return The absolute path where the dispatch files will be stored.
     */
    File getDispatchDestStorage();

    /**
     * Return the time (in 0.1ns ticks) of the first dispatched payload.
     *
     * @return first payload time
     */
    long getFirstDispatchedTime();

    /**
     * Get the stream metadata (currently number of dispatched events and
     * last dispatched time)
     *
     * @return metadata object
     */
    StreamMetaData getMetaData();

    /**
     * Get the number of bytes written to disk
     *
     * @return a long value ( number of bytes written to disk )
     */
    long getNumBytesWritten();

    /**
     * Get the  number of events dispatched during this run
     * @return a long value
     */
    long getNumDispatchedEvents();

    /**
     * Get the current run number
     * @return current run number
     */
    int getRunNumber();

    /**
     * Get the total number of events dispatched
     * @return a long value
     */
    long getTotalDispatchedEvents();

    /**
     * Does this dispatcher have one or more active STARTs?
     *
     * @return <tt>true</tt> if dispatcher has been started and not stopped
     */
    boolean isStarted();

    /**
     * Set the destination directory where the dispatch files will be saved.
     *
     * @param dirName The absolute path of directory where the dispatch files
     *                will be stored.
     */
    void setDispatchDestStorage(String dirName);

    /**
     * Set the maximum size of the dispatch file.
     *
     * @param maxFileSize the maximum size of the dispatch file.
     */
    void setMaxFileSize(long maxFileSize);

    /**
     * Returns the number of units still available in the disk (measured in MB).
     * If it fails to check the disk space, then it returns -1.
     *
     * @return the number of units still available in the disk.
     */
    long getDiskAvailable();

    /**
     * Returns the total number of units in the disk (measured in MB).
     * If it fails to check the disk space, then it returns -1.
     *
     * @return the total number of units in the disk.
     */
    long getDiskSize();
}
