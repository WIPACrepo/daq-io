/*
 * class: DispatchException
 *
 * Version $Id: DispatchException.java 2125 2007-10-12 18:27:05Z ksb $
 *
 * Date: March 31 2004
 *
 * (c) 2004 IceCube Collaboration
 */

package icecube.daq.io;

/**
 * This class is thrown when there is a problem with the Dispatch system.
 *
 * @author patton
 * @version $Id: DispatchException.java 2125 2007-10-12 18:27:05Z ksb $
 */
public class DispatchException
        extends Exception
{
    // constructors

    /**
     * Create an instance of this class.
     */
    public DispatchException()
    {
    }

    /**
     * Create an instance of this class.
     *
     * @param message a desciption of the exception.
     */
    public DispatchException(String message)
    {
        super(message);
    }

    /**
     * Create an instance of this class.
     *
     * @param message a desciption of the exception.
     * @param cause the reason this object was thrown.
     */
    public DispatchException(String message,
                                Throwable cause)
    {
        super(message,
              cause);
    }

    /**
     * Create an instance of this class.
     *
     * @param cause the reason this object was thrown.
     */
    public DispatchException(Throwable cause)
    {
        super(cause);
    }
}