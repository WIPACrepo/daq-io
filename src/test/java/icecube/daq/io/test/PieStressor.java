package icecube.daq.io.test;

import icecube.daq.io.PayloadInputEngine;
import icecube.daq.io.PayloadReceiveChannel;
import icecube.daq.payload.ByteBufferCache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cern.jet.random.Poisson;
import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;

class DevNullInputEngine extends PayloadInputEngine
{
    private ByteBufferCache cache;
    private static final Logger logger = Logger.getLogger(DevNullInputEngine.class);
    
    DevNullInputEngine(ByteBufferCache cache)
    {
        super("testEngine", 0, "test");
        this.cache = cache;
    }
    
    void recycleBuffers()
    {
        Iterator it = rcvChanList.iterator();
        while (it.hasNext())
        {
            PayloadReceiveChannel rcvChan = (PayloadReceiveChannel) it.next();
            try
            {
                while (!rcvChan.inputQueue.isEmpty())
                {
                    ByteBuffer buf = (ByteBuffer) rcvChan.inputQueue.take();
                    cache.returnBuffer(buf);
                }
            }
            catch (InterruptedException intx)
            {
                logger.error(intx);
            }
        }
    }
}

public class PieStressor 
{

    private DevNullInputEngine  engine;
    private ByteBufferCache     cache;
    private boolean             printChannelStates;
    
    private PieStressor()
    {
        cache  = new ByteBufferCache(256, 50000000L, 50000000L);
        engine = new DevNullInputEngine(cache);
        printChannelStates = false;
    }
    
    private void runTest() throws Exception
    {
        System.out.println("running test");
        engine.start();
        engine.startServer(cache);
        int port = engine.getServerPort();
        System.out.println("port is " + port);
        for (int i = 0; i < 20; i++)
        {
            Paygen pagan = new Paygen(port);
            Thread.sleep(100);
            pagan.start();
        }
        Thread.sleep(1000);
        engine.startProcessing();
        for (int i = 0; i < 1000; i++)
        {
            Thread.sleep(1000);
            Long[] brec = engine.getBytesReceived();
            for (int j = 0; j < brec.length; j++)
            {
                System.out.format("%10d ", brec[j]);
            }
            System.out.println();
            if (printChannelStates)
            {
                String[] states = engine.getPresentChannelStates();
                for (int j = 0; j < states.length; j++)
                {
                   System.out.format("%10s ", states[j]);
                }
                System.out.println();
            }
            // empty the byte buffers
            engine.recycleBuffers();
        }
        System.out.println("stopping engine.");
        engine.destroyProcessor();
    }
    
    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        PieStressor test = new PieStressor();
        test.runTest();
    }
    
}

class Paygen extends Thread
{
    private SocketChannel       socketChannel;
    private RandomEngine        randomEngine = new MersenneTwister(new java.util.Date());
    private Poisson             poissonDeviate;
    private int                 payloadSize = 40;
    private double              rate = 0.0;
    private static final Logger logger = Logger.getLogger(Paygen.class);
    
    Paygen(int port, double rate) throws IOException
    {
        socketChannel  = SocketChannel.open(new InetSocketAddress("localhost", port));
        poissonDeviate = new Poisson(1.0, randomEngine);
        this.rate      = rate;
    }
    
    Paygen(int port) throws IOException
    {
        // defaults to 1 kHz
        this(port, 1000.0);
    }
    
    public void run()
    {
        ByteBuffer buf = ByteBuffer.allocate(25000);
        long t0 = System.currentTimeMillis();
        try
        {
            for ( ; ; )
            {
                sleep(50);
                long t1 = System.currentTimeMillis();
                double dt = 0.001 * (t1 - t0);
                double mu = rate * dt;
                int nevt = poissonDeviate.nextInt(mu);
                int nreq = nevt * (payloadSize + 16);
                logger.debug("Generated " + nevt + " events.");
                buf.clear();
                if (nreq > buf.capacity()) buf = ByteBuffer.allocate(nreq);
                for (int k = 0; k < nevt; k++)
                {
                    buf.putInt(payloadSize+16);
                    buf.putInt(1001);
                    // TODO - put realistic clocks in here
                    buf.putLong(10000000L * t0);
                    for (int i = 0; i < payloadSize; i++)
                    {
                        buf.put((byte) i);
                    }
                }
                buf.flip();
                while (buf.remaining() > 0)
                    socketChannel.write(buf);
                t0 = t1;
            }
        }
        catch (InterruptedException intx)
        {
            return;
        }
        catch (IOException iox)
        {
            return;
        }
    }
}