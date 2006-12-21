/*
 * class: DispatchException
 *
 * Version $Id: DispatchException.java,v 1.1 2005/02/23 19:27:30 patton Exp $
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
 * @version $Id: DispatchException.java,v 1.1 2005/02/23 19:27:30 patton Exp $
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