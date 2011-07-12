package icecube.daq.io;

public interface DAQComponentIOProcess
{
    void destroyProcessor();
    void forcedStopProcessing();
    int getNumberOfChannels();
    String getPresentState();
    boolean isDestroyed();
    boolean isRunning();
    boolean isStopped();
    void registerComponentObserver(DAQComponentObserver compObserver);
    void start();
    void startProcessing();
}
