package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import java.nio.ByteBuffer;

/**
 * Keep track of the number of queued buffers
 */
public class DepthQueue
{
    /** internal queue */
    private LinkedQueue queue = new LinkedQueue();
    /** queue depth */
    private int depth;
    
    public DepthQueue()
    {
    }

    /**
     * Get the current queue depth.
     *
     * @return current depth
     */
    public int getDepth()
    {
        return depth;
    }

    /**
     * Is the queue empty?
     *
     * @return <tt>true</tt> if the queue is empty
     */
    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    /**
     * Add a buffer to the queue.
     *
     * @param buf buffer to be added
     *
     * @throws InterruptedException if the operation was interrupted
     */
    public void put(ByteBuffer buf)
        throws InterruptedException
    {
        synchronized (queue) {
            queue.put(buf);
            depth++;
        }
    }

    /**
     * Take a buffer from the queue.
     *
     * @return top buffer in the queue
     *
     * @throws InterruptedException if the operation was interrupted
     */
    public ByteBuffer take()
        throws InterruptedException
    {
        synchronized (queue) {
            ByteBuffer buf = (ByteBuffer) queue.take();
            depth--;
            return buf;
        }
    }
}
