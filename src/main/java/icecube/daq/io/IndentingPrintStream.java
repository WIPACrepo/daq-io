package icecube.daq.io;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Stack;

/**
 * This is a utility class that is intended add indenting
 * automatically for a PrintStream.
 * @author dwharton
 */
public class IndentingPrintStream extends PrintStream {

    protected PrintStream mt_PrintStream = System.out;
    protected Stack mt_Indents = new Stack();
    protected StringBuffer msb_CurrentIndent = new StringBuffer();
    protected Object mt_DefaultIndent = (Object) new String("  ");
    private boolean mbIndent;
    /**
     * constructor
     * @param tPrintStream print stream used for indenting
     */
    public IndentingPrintStream(PrintStream tPrintStream) {
        super(tPrintStream);
        mt_PrintStream = tPrintStream;
    }
    /**
     * Constructor (default)
     * Uses System.out by default.
     */
    public IndentingPrintStream() {
        super(System.out);
    }

    /**
     * Set's the default indent object.
     * @param tDefault Object whose toString() method is used for the default indent.
     */
    public void setDefaultIndentUnit(Object tDefault) {
        synchronized (this) {
            mt_DefaultIndent = tDefault;
        }
    }

    /**
     * builds the indent StringBuffer from the indent string.
     */
    protected void rebuildCurrentIndent() {
        synchronized (this) {
            msb_CurrentIndent.setLength(0);
            Iterator iter = mt_Indents.iterator();
            while (iter.hasNext()) {
                msb_CurrentIndent.append(iter.next().toString());
            }
        }
    }
    /**
     * Adds an additional indent based on the default indent.
     */
    public void addIndent() {
        addIndent(mt_DefaultIndent);
    }
    /**
     * removes a level of indentation.
     */
    public void undent() {
        synchronized(this) {
            if (mt_Indents.size() > 0) {
                Object tUndent = mt_Indents.pop();
                if (mt_Indents.size() > 0) {
                    int iNewSize = msb_CurrentIndent.length() - tUndent.toString().length();
                    if (iNewSize > 0) {
                        msb_CurrentIndent.setLength(iNewSize);
                    } else {
                        rebuildCurrentIndent();
                    }
                } else {
                    msb_CurrentIndent.setLength(0);
                }
            }
        }
    }
    /**
     * indents the PrintStreamBefore the next output.
     */
    protected void indent() {
        if (mbIndent) mt_PrintStream.print(msb_CurrentIndent.toString());
        mbIndent = false;
    }
    /**
     * Adds an indent as specified with the indent-object.
     * the Object's toString() method must return the intended indent string
     */
    public void addIndent(Object tNewIndent) {
        synchronized (this) {
            if (tNewIndent != null) {
                mt_Indents.push(tNewIndent);
                msb_CurrentIndent.append(tNewIndent.toString());
            }
        }
    }
    /**
     * Close the stream.  This is done by flushing the stream and then closing
     * the underlying output stream.
     *
     * @see        java.io.OutputStream#close()
     */
    public void close() {
        mt_PrintStream.close();
    }
    /**
     * Flush the stream.  This is done by writing any buffered output bytes to
     * the underlying output stream and then flushing that stream.
     *
     * @see        java.io.OutputStream#flush()
     */
    public void flush() {
        mt_PrintStream.flush();
    }
    /**
     * Write <code>len</code> bytes from the specified byte array starting at
     * offset <code>off</code> to this stream.  If automatic flushing is
     * enabled then the <code>flush</code> method will be invoked.
     *
     * <p> Note that the bytes will be written as given; to write characters
     * that will be translated according to the platform's default character
     * encoding, use the <code>print(char)</code> or <code>println(char)</code>
     * methods.
     *
     * @param  buf   A byte array
     * @param  off   Offset from which to start taking bytes
     * @param  len   Number of bytes to write
     */
    public void write(byte[] buf, int off, int len) {
        indent();
        mt_PrintStream.write(buf, off, len);
    }
    /**
     * Flush the stream and check its error state.  The internal error state
     * is set to <code>true</code> when the underlying output stream throws an
     * <code>IOException</code> other than <code>InterruptedIOException</code>,
     * and when the <code>setError</code> method is invoked.  If an operation
     * on the underlying output stream throws an
     * <code>InterruptedIOException</code>, then the <code>PrintStream</code>
     * converts the exception back into an interrupt by doing:
     * <pre>
     *     Thread.currentThread().interrupt();
     * </pre>
     * or the equivalent.
     *
     * @return True if and only if this stream has encountered an
     *         <code>IOException</code> other than
     *         <code>InterruptedIOException</code>, or the
     *         <code>setError</code> method has been invoked
     */
    public boolean checkError() {
        return mt_PrintStream.checkError();
    }
    /**
     * Write the specified byte to this stream.  If the byte is a newline and
     * automatic flushing is enabled then the <code>flush</code> method will be
     * invoked.
     *
     * <p> Note that the byte is written as given; to write a character that
     * will be translated according to the platform's default character
     * encoding, use the <code>print(char)</code> or <code>println(char)</code>
     * methods.
     *
     * @param  b  The byte to be written
     * @see #print(char)
     * @see #println(char)
     */
    public void write(int b) {
        indent();
        mt_PrintStream.write(b);
    }
    /**
     * Print a boolean value.  The string produced by <code>{@link
     * java.lang.String#valueOf(boolean)}</code> is translated into bytes
     * according to the platform's default character encoding, and these bytes
     * are written in exactly the manner of the
     * <code>{@link #write(int)}</code> method.
     *
     * @param      b   The <code>boolean</code> to be printed
     */
    public void print(boolean b) {
        indent();
        mt_PrintStream.print(b);
    }
    /**
     * Print an integer.  The string produced by <code>{@link
     * java.lang.String#valueOf(int)}</code> is translated into bytes
     * according to the platform's default character encoding, and these bytes
     * are written in exactly the manner of the
     * <code>{@link #write(int)}</code> method.
     *
     * @param      i   The <code>int</code> to be printed
     * @see        java.lang.Integer#toString(int)
     */
    public void print(int i) {
        indent();
        mt_PrintStream.print(i);
    }
    /**
     * Print a floating-point number.  The string produced by <code>{@link
     * java.lang.String#valueOf(float)}</code> is translated into bytes
     * according to the platform's default character encoding, and these bytes
     * are written in exactly the manner of the
     * <code>{@link #write(int)}</code> method.
     *
     * @param      f   The <code>float</code> to be printed
     * @see        java.lang.Float#toString(float)
     */
    public void print(float f) {
        indent();
        mt_PrintStream.print(f);
    }
    /**
     * Print a long integer.  The string produced by <code>{@link
     * java.lang.String#valueOf(long)}</code> is translated into bytes
     * according to the platform's default character encoding, and these bytes
     * are written in exactly the manner of the
     * <code>{@link #write(int)}</code> method.
     *
     * @param      l   The <code>long</code> to be printed
     * @see        java.lang.Long#toString(long)
     */
    public void print(long l) {
        indent();
        mt_PrintStream.print(l);
    }
    /**
     * Print an array of characters.  The characters are converted into bytes
     * according to the platform's default character encoding, and these bytes
     * are written in exactly the manner of the
     * <code>{@link #write(int)}</code> method.
     *
     * @param      s   The array of chars to be printed
     *
     * @throws  NullPointerException  If <code>s</code> is <code>null</code>
     */
    public void print(char[] s) {
        indent();
        mt_PrintStream.print(s);
    }
    /**
     * Print a character.  The character is translated into one or more bytes
     * according to the platform's default character encoding, and these bytes
     * are written in exactly the manner of the
     * <code>{@link #write(int)}</code> method.
     *
     * @param      c   The <code>char</code> to be printed
     */
    public void print(char c) {
        indent();
        mt_PrintStream.print(c);
    }
    /**
     * Terminate the current line by writing the line separator string.  The
     * line separator string is defined by the system property
     * <code>line.separator</code>, and is not necessarily a single newline
     * character (<code>'\n'</code>).
     */
    public void println() {
        indent();
        mt_PrintStream.println();
        mbIndent=true;
    }
    /**
     * Print a boolean and then terminate the line.  This method behaves as
     * though it invokes <code>{@link #print(boolean)}</code> and then
     * <code>{@link #println()}</code>.
     *
     * @param x  The <code>boolean</code> to be printed
     */
    public void println(boolean x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Print a double-precision floating-point number.  The string produced by
     * <code>{@link java.lang.String#valueOf(double)}</code> is translated into
     * bytes according to the platform's default character encoding, and these
     * bytes are written in exactly the manner of the <code>{@link
     * #write(int)}</code> method.
     *
     * @param      d   The <code>double</code> to be printed
     * @see        java.lang.Double#toString(double)
     */
    public void print(double d) {
        indent();
        mt_PrintStream.print(d);
    }
    /**
     * Print an integer and then terminate the line.  This method behaves as
     * though it invokes <code>{@link #print(int)}</code> and then
     * <code>{@link #println()}</code>.
     *
     * @param x  The <code>int</code> to be printed.
     */
    public void println(int x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Print a float and then terminate the line.  This method behaves as
     * though it invokes <code>{@link #print(float)}</code> and then
     * <code>{@link #println()}</code>.
     *
     * @param x  The <code>float</code> to be printed.
     */
    public void println(float x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Print an Object and then terminate the line.  This method behaves as
     * though it invokes <code>{@link #print(Object)}</code> and then
     * <code>{@link #println()}</code>.
     *
     * @param x  The <code>Object</code> to be printed.
     */
    public void println(Object x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Print a String and then terminate the line.  This method behaves as
     * though it invokes <code>{@link #print(String)}</code> and then
     * <code>{@link #println()}</code>.
     *
     * @param x  The <code>String</code> to be printed.
     */
    public void println(String x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Print a character and then terminate the line.  This method behaves as
     * though it invokes <code>{@link #print(char)}</code> and then
     * <code>{@link #println()}</code>.
     *
     * @param x  The <code>char</code> to be printed.
     */
    public void println(char x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Print an array of characters and then terminate the line.  This method
     * behaves as though it invokes <code>{@link #print(char[])}</code> and
     * then <code>{@link #println()}</code>.
     *
     * @param x  an array of chars to print.
     */
    public void println(char[] x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Print a double and then terminate the line.  This method behaves as
     * though it invokes <code>{@link #print(double)}</code> and then
     * <code>{@link #println()}</code>.
     *
     * @param x  The <code>double</code> to be printed.
     */
    public void println(double x) {
        indent();
        mt_PrintStream.println(x);
        mbIndent=true;
    }
    /**
     * Writes <code>b.length</code> bytes to this output stream.
     * <p>
     * The <code>write</code> method of <code>FilterOutputStream</code>
     * calls its <code>write</code> method of three arguments with the
     * arguments <code>b</code>, <code>0</code>, and
     * <code>b.length</code>.
     * <p>
     * Note that this method does not call the one-argument
     * <code>write</code> method of its underlying stream with the single
     * argument <code>b</code>.
     *
     * @param      b   the data to be written.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.FilterOutputStream#write(byte[], int, int)
     */
    public void write(byte[] b) throws IOException {
        indent();
        mt_PrintStream.write(b);
    }
}
