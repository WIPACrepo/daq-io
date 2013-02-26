package icecube.daq.io.test;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.DAQComponentObserver;
import icecube.daq.io.ErrorState;
import icecube.daq.io.NormalState;

public class MockObserver
    implements DAQComponentObserver
{
    private String name;

    private String sinkId;
    private boolean sinkStopCalled;
    private boolean sinkErrorCalled;

    private String sourceId;
    private boolean sourceStopCalled;
    private boolean sourceErrorCalled;

    public MockObserver(String name)
    {
        this.name = name;
    }

    public boolean gotSinkError()
    {
        return sinkErrorCalled;
    }

    public boolean gotSinkStop()
    {
        return sinkStopCalled;
    }

    public boolean gotSourceError()
    {
        return sourceErrorCalled;
    }

    public boolean gotSourceStop()
    {
        return sourceStopCalled;
    }

    public void setSinkNotificationId(String id)
    {
        sinkId = id;
    }

    public void setSourceNotificationId(String id)
    {
        sourceId = id;
    }

    public synchronized void update(Object object, String notificationId)
    {
        if (object instanceof NormalState) {
            NormalState state = (NormalState)object;
            if (state == NormalState.STOPPED) {
                if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                    notificationId.equals(sourceId))
                {
                    sourceStopCalled = true;
                } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                           notificationId.equals(sinkId))
                {
                    sinkStopCalled = true;
                } else {
                    throw new Error("Unexpected " + name +
                                    " stop notification \"" +
                                    notificationId + "\"");
                }
            } else if (state == NormalState.RUNNING ||
                       state == NormalState.DESTROYED)
            {
                // ignore some standard states
            } else {
                throw new Error("Unexpected " + name + " notification state " +
                                state);
            }
        } else if (object instanceof ErrorState) {
            ErrorState state = (ErrorState)object;
            if (state == ErrorState.UNKNOWN_ERROR) {
                if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                    notificationId.equals(sourceId))
                {
                    sourceErrorCalled = true;
                } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                           notificationId.equals(sinkId))
                {
                    sinkErrorCalled = true;
                } else {
                    throw new Error("Unexpected " + name +
                                    " error notification \"" +
                                    notificationId + "\"");
                }
            } else {
                throw new Error("Unexpected " + name + " notification state " +
                                state);
            }
        } else {
            throw new Error("Unexpected " + name + " notification object " +
                            object.getClass().getName());
        }
    }

    public String toString()
    {
        return "Observer[name=" + name +
            ":Sink=" + sinkId +
            (sinkStopCalled ? ",stop" : ",!stop") +
            (sinkErrorCalled ? ",error" : ",!error") + "]" +
            ":Src=" + sourceId +
            (sourceStopCalled ? ",stop" : ",!stop") +
            (sourceErrorCalled ? ",error" : ",!error") + "]";
    }
}
