package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.nio.channels.WritableByteChannel;

/**
 * input/output channel.
 */
public interface IOChannel
{
    /**
     * Register an observer for this channel.
     *
     * @param compObserver observer
     * @param notificationID string which is sent with all notifications
     */
    void registerComponentObserver(DAQComponentObserver compObserver,
                                   String notificationID);
}
