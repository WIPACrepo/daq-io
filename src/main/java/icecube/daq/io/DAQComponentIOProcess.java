package icecube.daq.io;

import icecube.daq.common.DAQComponentObserver;

public interface DAQComponentIOProcess
{
    public void destroyProcessor();
    public void forcedStopProcessing();
    public String getPresentState();
    public boolean isDestroyed();
    public boolean isRunning();
    public boolean isStopped();
    public void registerComponentObserver(DAQComponentObserver compObserver);
    public void start();
    public void startProcessing();
}
