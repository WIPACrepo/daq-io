package icecube.daq.io.test;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.ErrorState;
import icecube.daq.common.NormalState;

public class MockObserver
    implements DAQComponentObserver
{
    private String sinkNotificationId;
    private boolean sinkStopNotificationCalled;
    private boolean sinkErrorNotificationCalled;

    private String sourceNotificationId;
    private boolean sourceStopNotificationCalled;
    private boolean sourceErrorNotificationCalled;

    public boolean gotSinkError()
    {
        return sinkErrorNotificationCalled;
    }

    public boolean gotSinkStop()
    {
        return sinkStopNotificationCalled;
    }

    public boolean gotSourceError()
    {
        return sourceErrorNotificationCalled;
    }

    public boolean gotSourceStop()
    {
        return sourceStopNotificationCalled;
    }

    public void setSinkNotificationId(String id)
    {
        sinkNotificationId = id;
    }

    public void setSourceNotificationId(String id)
    {
        sourceNotificationId = id;
    }

    public synchronized void update(Object object, String notificationId)
    {
        if (object instanceof NormalState) {
            NormalState state = (NormalState)object;
            if (state == NormalState.STOPPED) {
                if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                    notificationId.equals(sourceNotificationId))
                {
                    sourceStopNotificationCalled = true;
                } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                           notificationId.equals(sinkNotificationId))
                {
                    sinkStopNotificationCalled = true;
                } else {
                    throw new Error("Unexpected stop notification \"" +
                                    notificationId + "\"");
                }
            } else {
                throw new Error("Unexpected notification state " +
                                state);
            }
        } else if (object instanceof ErrorState) {
            ErrorState state = (ErrorState)object;
            if (state == ErrorState.UNKNOWN_ERROR) {
                if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                    notificationId.equals(sourceNotificationId))
                {
                    sourceErrorNotificationCalled = true;
                } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                           notificationId.equals(sinkNotificationId))
                {
                    sourceStopNotificationCalled = true;
                } else {
                    throw new Error("Unexpected error notification \"" +
                                    notificationId + "\"");
                }
            } else {
                throw new Error("Unexpected notification state " +
                                state);
            }
        } else {
            throw new Error("Unexpected notification object " +
                            object.getClass().getName());
        }
    }
}


