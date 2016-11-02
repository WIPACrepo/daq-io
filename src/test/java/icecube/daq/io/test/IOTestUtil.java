package icecube.daq.io.test;

import icecube.daq.io.DAQComponentIOProcess;
import icecube.daq.io.DAQComponentInputProcessor;
import icecube.daq.io.DAQComponentOutputProcess;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import junit.framework.Assert;

public final class IOTestUtil
    extends Assert
{
    private static final int REPS = 500;
    private static final int SLEEP_TIME = 10;

    private static ByteBuffer stopMsg;

    public static final void sendStopMsg(WritableByteChannel sinkChannel)
        throws IOException
    {
        if (stopMsg == null) {
            stopMsg = ByteBuffer.allocate(4);
            stopMsg.putInt(0, 4);
            stopMsg.limit(4);
        }

        stopMsg.position(0);
        sinkChannel.write(stopMsg);
    }

    public static final void waitUntilConnected(DAQComponentOutputProcess proc)
    {
        waitUntilConnected(proc, "");
    }

    public static final void waitUntilConnected(DAQComponentOutputProcess proc,
                                                String extra)
    {
        for (int i = 0; i < REPS && !proc.isConnected(); i++) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("OutputProcess did not die after disconnect" + extra,
                   proc.isConnected());
    }

    public static final void waitUntilDestroyed(DAQComponentIOProcess proc)
    {
        waitUntilDestroyed(proc, "");
    }

    public static final void waitUntilDestroyed(DAQComponentIOProcess proc,
                                                String extra)
    {
        for (int i = 0; i < REPS && !proc.isDestroyed(); i++) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("IOProcess did not die after kill request" + extra,
                   proc.isDestroyed());
    }

    public static final void waitUntilDisposing(DAQComponentInputProcessor proc)
    {
        waitUntilDisposing(proc, "");
    }

    public static final void waitUntilDisposing(DAQComponentInputProcessor proc,
                                                String extra)
    {
        for (int i = 0; i < REPS && !proc.isDisposing(); i++) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("InputProcess in " + proc.getPresentState() +
                   ", not Disposing after DisposeSig" + extra,
                   proc.isDisposing());
    }

    public static final void waitUntilRunning(DAQComponentIOProcess proc)
    {
        waitUntilRunning(proc, "");
    }

    public static final void waitUntilRunning(DAQComponentIOProcess proc,
                                              String extra)
    {
        for (int i = 0; i < REPS && !proc.isRunning(); i++) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("IOProcess in " + proc.getPresentState() +
                   ", not Running after StartSig" + extra, proc.isRunning());
    }

    public static final void waitUntilStopped(DAQComponentIOProcess proc,
                                              String action)
    {
        waitUntilStopped(proc, action, "");
    }

    public static final void waitUntilStopped(DAQComponentIOProcess proc,
                                              String action,
                                              String extra)
    {
        for (int i = 0; i < REPS && !proc.isStopped(); i++) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("IOProcess in " + proc.getPresentState() +
                   ", not Idle after " + action + extra, proc.isStopped());
    }
}
