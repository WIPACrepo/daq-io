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
 * @version $Id: ErrorState.java 17114 2018-09-26 09:51:56Z dglo $
 */
public final class ErrorState {

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

    @Override
    public String toString() {
        return myName;
    }
}
