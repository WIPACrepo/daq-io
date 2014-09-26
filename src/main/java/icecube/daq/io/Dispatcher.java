/*
 * interface: Dispatcher
 *
 * Version $Id: Dispatcher.java 15168 2014-09-26 17:39:14Z dglo $
 *
 * Date: April 1 2004
 *
 * (c) 2004 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IWriteablePayload;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * This interface specifies how events are dispatched from the DAQ system.
 *
 * @author patton
 * @version $Id: Dispatcher.java 15168 2014-09-26 17:39:14Z dglo $
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
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    void dataBoundary() throws DispatchException;

    /**
     * Signals to the dispatch system that the set of events that preced this
     * call are separated, by some criteria, for those that succeed it.
     * <p/>
     * The message supplied with this method is opaque to the system, i.e. it
     * is not used by the system, and it simple passed on through the any
     * delivery client.
     *
     * @param message a String explaining the reason for the boundary.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    void dataBoundary(String message)throws DispatchException;

    /**
     * Copies the event in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     *
     * @param buffer the ByteBuffer containg the event.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    void dispatchEvent(ByteBuffer buffer) throws DispatchException;

    /**
     * Dispatch a Payload event object
     *
     * @param event A payload object.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    void dispatchEvent(IWriteablePayload event) throws DispatchException;

    /**
     * Copies the events in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     * <p/>
     * The number of events is taken to be the length of the indices array.
     *
     * @param buffer the ByteBuffer containg the events.
     * @param indices the 'position' of each event inside the buffer.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    void dispatchEvents(ByteBuffer buffer, int[] indices)throws DispatchException;

    /**
     * Copies the events in the buffer into this object. The buffer should be
     * prepared for reading so normally a {@link ByteBuffer#flip flip} should
     * be done before this call and a {@link ByteBuffer#compact compact}
     * afterwards.
     *
     * @param buffer the ByteBuffer containg the events.
     * @param indices the 'position' of each event inside the buffer.
     * @param count the number of events, this must be less that the length of
     * the indices array.
     * @throws DispatchException is there is a problem in the Dispatch system.
     */
    void dispatchEvents(ByteBuffer buffer, int[] indices, int count)throws DispatchException;

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
     * Get the  number of events dispatched during this run
     * @return a long value
     */
    long getNumDispatchedEvents();

    /**
     * Get the total number of events dispatched
     * @return a long value
     */
    long getTotalDispatchedEvents();

    /**
     * Get the number of bytes written to disk
     *
     * @return a long value ( number of bytes written to disk )
     */
    long getNumBytesWritten();

    /**
     * Does this dispatcher have one or more active STARTs?
     *
     * @return <tt>true</tt> if dispatcher has been started and not stopped
     */
    boolean isStarted();

    /**
     * Set the destination directory where the dispatch files will be saved.
     *
     * @param dirName The absolute path of directory where the dispatch files will be stored.
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
