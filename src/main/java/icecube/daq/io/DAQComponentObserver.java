/**
 * DAQComponentObserver
 * Date: Nov 13, 2006 3:32:14 PM
 *
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

/**
 * This interface represent the interaction between the input/output engines and the high level
 * component manager
 * @author artur
 * @version $Id: DAQComponentObserver.java 2629 2008-02-11 05:48:36Z dglo $
 */
public interface DAQComponentObserver {

    /**
     * The observable object notifies this class that its state type has changed. The class that
     * implements this interface should synchronize this method if more than one objects are
     * using it.
     * @param object
     */
    void update(Object object, String notificationID);
}
