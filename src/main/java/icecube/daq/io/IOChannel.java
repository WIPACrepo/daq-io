package icecube.daq.io;

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
