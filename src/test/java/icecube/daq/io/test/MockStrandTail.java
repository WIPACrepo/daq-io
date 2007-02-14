package icecube.daq.io.test;

import icecube.daq.splicer.ClosedStrandException;
import icecube.daq.splicer.OrderingException;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.StrandTail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MockStrandTail
    implements StrandTail
{
    private ArrayList entries = new ArrayList();
    private boolean closed;

    public MockStrandTail()
    {
    }

    /**
     * Closes the associated {@link Strand}. The Splicer will continue to
     * handle those Spliceables already pushed into this object but will not
     * acccept any more. Any further attempt to push in a Spliceable into this
     * object will cause a ClosedStrandException to be thrown.
     * <p/>
     * If the associated Strand is already closed then invoking this method
     * will have no effect.
     */
    public void close()
    {
        closed = true;
    }

    /**
     * Returns the {@link Spliceable} at the "head" of this object without
     * removing it from this object. If this object is currently empty this
     * method will return <code>null</code>.
     *
     * @return the Spliceable at the "head" of this object.
     */
    public Spliceable head()
    {
        return (Spliceable) entries.get(0);
    }

    /**
     * Returns true if the {@link #close()} method has been called on this
     * object.
     *
     * @return true if this object is closed.
     */
    public boolean isClosed()
    {
        return closed;
    }

    /**
     * Adds the specified List of {@link Spliceable} objects onto the tail of
     * the associated {@link Strand}. The List of Spliceables must be ordered
     * such that all Spliceable, <code>s</code>, - with the exception of the
     * {@link Splicer#LAST_POSSIBLE_SPLICEABLE} object - that are lower in the
     * list than Spliceable <code>t</code> are also less or equal to
     * <code>t</code>,
     * <p/>
     * <pre>
     *    0 > s.compareTo(t)
     * </pre>
     * <p/>
     * otherwise an IllegalArgumentException will be thrown.
     * <p/>
     * Moreover the first Spliceable in the List must be greater or equal to
     * the last Spliceable - again, with the exception of the
     * <code>LAST_POSSIBLE_SPLICEABLE</code> object - pushed into this object
     * otherwise an IllegalArgumentException will be thrown.
     *
     * @param spliceables the List of Spliceable objects to be added.
     * @return this object, so that pushes can be chained.
     * @throws OrderingException if the specified List of Spliceables is not
     * properly ordered or is mis-ordered with respect to Spliceables already
     * pushed into this object
     * @throws ClosedStrandException is the associated Strand has been closed.
     */
    public StrandTail push(List splList)
        throws OrderingException, ClosedStrandException
    {
        for (Iterator iter = splList.iterator(); iter.hasNext(); ) {
            push((Spliceable) iter.next());
        }

        return this;
    }

    /**
     * Adds the specified {@link Spliceable} onto the tail of the associated
     * {@link Strand}. The specified Spliceable must be greater or equal to all
     * other Spliceables, <code>s</code>, - with the exception of the {@link
     * Splicer#LAST_POSSIBLE_SPLICEABLE} object - that have been previously
     * pushed into this object,
     * <p/>
     * <pre>
     *    0 > s.compareTo(spliceable)
     * </pre>
     * <p/>
     * otherwise an IllegalArgumentException will be thrown.
     * <p/>
     * Any Spliceables pushed into the Strand after a <code>LAST_POSSIBLE_SPLICE
ABLE</code>
     * object will not appear in the associated Strand until the Splicer has
     * "stopped".
     *
     * @param spliceable the Spliceable to be added.
     * @return this object, so that pushes can be chained.
     * @throws OrderingException if the specified Spliceable is mis-ordered
     * with respect to Spliceables already pushed into this object
     * @throws ClosedStrandException is the assoicated Strand has been closed.
     */
    public StrandTail push(Spliceable spl)
        throws OrderingException, ClosedStrandException
    {
        entries.add(spl);

        return this;
    }

    /**
     * Returns the number of {@link Spliceable} objects pushed into this object
     * that have yet to be woven into the resultant rope.
     *
     * @return the number of {@link Spliceable} objects yet to be woven.
     */
    public int size()
    {
        return entries.size();
    }

    public String toString()
    {
        return "MockStrandTail[" + entries.size() + "," +
            (closed ? "" : "!") + "closed]";
    }
}
