/**
 * ErrorState.java
 * Date: Nov 13, 2006 4:02:27 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

/**
 * This class is an enumeration of DAQ error types.
 *
 * @author artur
 * @version $Id: ErrorState.java 2228 2007-11-02 16:02:54Z dglo $
 */
public class ErrorState {

    // TODO: Add more state errors here as needed
    public static final ErrorState SELECTOR_ERROR = new ErrorState("SELECTOR_ERROR");
    public static final ErrorState CHANNEL_ERROR = new ErrorState("CHANNEL_ERROR");
    public static final ErrorState MUTEX_ERROR = new ErrorState("MUTEX_ERROR");
    public static final ErrorState DATA_ERROR = new ErrorState("DATA_ERROR");
    public static final ErrorState UNKNOWN_ERROR = new ErrorState("UNKNOWN_ERROR");

    private final String myName; // for debug only

    private ErrorState(String name) {
        myName = name;
    }

    public String toString() {
        return myName;
    }
}
