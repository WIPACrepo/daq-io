package icecube.daq.io.test;

import icecube.daq.splicer.OrderingException;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SplicedAnalysis;
import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.SplicerListener;
import icecube.daq.splicer.StrandTail;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MockSplicer
    implements Splicer<Spliceable>
{
    private ArrayList strandList = new ArrayList();
    private State state;

    public MockSplicer()
    {
        state = State.STOPPED;
    }

    /**
     * Adds the specified channel to this object so its data can be used to
     * construct Spliceable objects. The channel can only be added when this
     * object is in the Stopped state. If the channel has already been added
     * then this method will have no effect.
     * <p>
     * The channel must implement the ReadableByteChannel interface.
     * <p>
     * This method is optional, but should have a matching {@link
     * #removeSpliceableChannel(SelectableChannel)} method if it is
     * implemented. If it is not implemented then a UnsupportedOperationExceptio
n
     * is thrown by this method.
     *
     * @param channel the channel to be added.
     * @throws IllegalArgumentException is channel does not implement
     * ReadableByteChannel interface.
     * @throws IOException if the channel can not be made non-blocking.
     * @throws UnsupportedOperationException if the implementation does not
     * support this method.
     */
    public void addSpliceableChannel(SelectableChannel channel)
        throws IOException
    {
        throw new Error("Unimplemented");
    }

    /**
     * The specified SplicerListener will receive SplicerChangedEvent objects.
     *
     * @param listener the SplicerListener to add.
     */
    @Override
    public void addSplicerListener(SplicerListener listener)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Request that <code>execute</code> method of this object's {@link
     * SplicedAnalysis} is invoked with the current "rope". This method will
     * block until that method returns.
     * <p>
     * It should be noted that the <code>execute</code> method may be executed
     * automatically between the time this method is invoked and requested
     * invocation of <code>execute</code> takes place. The automatic execution
     * will not affect this method, which <em>will continue to block</em> until
     * the requested execution has completed.
     * <p>
     * <b>Warning:</b> This method must never be called from within it own
     * analysis <code>execute</code> method as this may cause a deadlock! (All
     * <code>execute</code> invocation are allowed to execute in the same
     * Thread and thus calling <code>analyze</code> from within the
     * <code>execute</code> will block the Thread, while waiting for what could
     * be the same Thread to execute!.)
     */
    public void analyze()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Adds a new Strand to this object. The returned {@link StrandTail} can
     * be used by the client to push new {@link Spliceable}s into the new
     * Strand and to close that Strand when it is no longer needed.
     *
     * @return the StrandTail used to push Spliceable into the new Strand.
     */
    @Override
    public StrandTail beginStrand()
    {
        StrandTail tail = new MockStrandTail();
        strandList.add(tail);
        return tail;
    }

    /**
     * Frees up the resources used by this object. After ths method has been
     * invoke the behavor of any method in this interface, except those
     * dealting directly with state, will be undetermined.
     */
    @Override
    public void dispose()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Requests that this object stop weaving data from all of its Strands.
     * <p>
     * This method does not wait for Spliceables already pushed into this
     * object to be woven, but rather stops weaving as soon as possible. Those
     * Spliceable already pushed but not woven will be handled when this object
     * is re-started.
     * <p>
     * If this object has already stopped then this method will have no
     * effect.
     */
    @Override
    public void forceStop()
    {
        state = State.STOPPED;
    }

    /**
     * Returns the {@link SplicedAnalysis} that is being used by this object.
     *
     * @return the {@link SplicedAnalysis} that is being used by this object.
     */
    @Override
    public SplicedAnalysis getAnalysis()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Returns the current state of this object.
     *
     * @return the current state of this object.
     */
    @Override
    public State getState()
    {
        return state;
    }

    /**
     * Returns the number of open Strands that are in this object.
     *
     * @return the number of open Strands.
     */
    @Override
    public int getStrandCount()
    {
        return strandList.size();
    }

    public Iterator iterator()
    {
        return strandList.iterator();
    }

    /**
     * Returns the List of {@link SelectableChannel} objects on which this
     * object is waiting before it can weave any more rope.
     * <p>
     * <b>Warning:</b> This method must never be called from within it own
     * analysis <code>execute</code> method as this may cause a deadlock. As
     * the results of this method are internal data from the Splicer, it may
     * need to finished executing any analysis before copy out this data and
     * thus could cause a deadlock.
     * <p>
     * This method is optional, but should have a matching {@link
     * #addSpliceableChannel(SelectableChannel)} and {@link
     * #removeSpliceableChannel(SelectableChannel)} methods if it is
     * implemented. If it is not implemented then a UnsupportedOperationExceptio
n
     * is thrown by this method.
     *
     * @return a List of StrandTail objects
     * @throws UnsupportedOperationException if the implementation does not
     * support this method.
     */
    public List pendingChannels()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Returns the List of {@link StrandTail} objects on which this object is
     * waiting before it can weave any more rope.
     * <p>
     * <b>Warning:</b> This method must never be called from within it own
     * analysis <code>execute</code> method as this may cause a deadlock. As
     * the results of this method are internal data from the Splicer, it may
     * need to finished executing any analysis before copy out this data and
     * thus could cause a deadlock.
     *
     * @return a List of StrandTail objects
     */
    public List pendingStrands()
    {
        return new ArrayList(strandList);
    }

    /**
     * Removes the specified channel from this object so its data can no longer
     * be used in the construction of the List of Spliceable objects. The
     * channel can only be removed when this object is in the Stopped state. If
     * the channel has not been added then this method will have no effect.
     * <p>
     * This method is optional, but should have a matching {@link
     * #addSpliceableChannel(SelectableChannel)} method if it is implemented.
     * If it is not implemented then a UnsupportedOperationException is thrown
     * by this method.
     *
     * @param channel the channel to be removed.
     * @throws UnsupportedOperationException if the implementation does not
     * support this method.
     */
    public void removeSpliceableChannel(SelectableChannel channel)
    {
        throw new Error("Unimplemented");
    }

    /**
     * The specified SplicerListener will no longer receive SplicerChangedEvent
     * objects.
     *
     * @param listener the SplicerListener to remove.
     */
    @Override
    public void removeSplicerListener(SplicerListener listener)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Requests that this object start weaving data from all of its Strands.
     * <p>
     * This method will produce a "frayed" start such that there is no
     * guarantee that the initial Spliceables handed to the analysis object are
     * greater than or equal to the first Spliceable in each Strand. However it
     * is guaranteed that the analysis object will not be invoked until at
     * least one Spliceable has been seen in each Strand.
     * <p>
     * If this object has already started, or is in the process of starting
     * then this method will have no effect.
     *
     * @throws IllegalStateException if this object is not in a state from
     * which it can be started.
     */
    @Override
    public void start()
    {
        state = State.STARTED;
    }

    /**
     * Requests that this object start weaving data from all of its Strands.
     * <p>
     * This method will produce a "clean cut" start such that all Strands have
     * at least one Spliceable that is less than or equal to the "beginning"
     * Spliceable. The "beginning" Spliceable is defined as the greater of
     * either the specified Spliceable or the first Spliceable in one Strand
     * which is greater than or equal to the first Spliceable in all other
     * Strands (excluding those Strand whose first Spliceable is a
     * LAST_POSSIBLE_SPLICEABLE). Neither <code>null</code> nor the
     * <code>LAST_POSSIBLE_SPLICEABLE</code> object are valid arguments and
     * will cause an exception to be thrown.
     * <p>
     * If this object has already started, or is in the process of starting
     * then this method will have no effect.
     * <p>
     * <em>note:</em> This method will discard and Spliceables that are less
     * than the "beginning" Spliceable.
     *
     * @param start all Spliceables handled to the analysis routine are
     * guaranteed to be greater than or euqal to this object.
     * @throws IllegalStateException if this object is not in a state from
     * which it can be started.
     * @throws IllegalArgumentException if the specified Spliceable is the
     * <code>LAST_POSSIBLE_SPLICEABLE</code> object.
     * @throws NullPointerException if <code>start</code> in null.
     */
    public void start(Spliceable start)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Requests that this object stop weaving data from all of its Strands.
     * <p>
     * This method will produce a "frayed" stop such that there is no guarantee
     * that the final Spliceables handed to the analysis object are less than
     * or equal to the last Spliceable in each Strand.
     * <p>
     * If this object has already stopped, or is in the process of stopping,
     * then this method will have no effect.
     *
     * @throws IllegalStateException if this object is not in a state from
     * which it can be stopped.
     */
    @Override
    public void stop()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Requests that this object stop weaving data from all of its Strands.
     * <p>
     * This method will produce a "clean cut" stop such that all Strands have
     * at least one Spliceable that is greater than the specified Spliceable.
     * For (hopefully) obvious reasons the means that neither <code>null</code>
     * nor the <code>LAST_POSSIBLE_SPLICEABLE</code> object are valid arguments
     * and will cause an exception to be thrown.
     * <p>
     * If this object has already stopped, or is in the process of stopping,
     * then this method will have no effect.
     *
     * @param stop all Spliceables handled to the analysis routine are
     * guaranteed to be less than or euqal to this object.
     * @throws OrderingException if the specified stop is less than the last
     * Spliceable that was weaved (making the requested clean stop
     * impossible.)
     * @throws IllegalStateException if this object is not in a state from
     * which it can be stopped.
     * @throws IllegalArgumentException if the specified Spliceable is the
     * <code>LAST_POSSIBLE_SPLICEABLE</code> object.
     * @throws NullPointerException if <code>stop</code> in null.
     */
    public void stop(Spliceable stop)
        throws OrderingException
    {
        throw new Error("Unimplemented");
    }
}
