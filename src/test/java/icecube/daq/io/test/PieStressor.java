package icecube.daq.io.test;

import icecube.daq.io.InputChannel;
import icecube.daq.io.InputChannelParent;
import icecube.daq.io.PayloadInputEngine;
import icecube.daq.io.PayloadReader;
import icecube.daq.io.PayloadReceiveChannel;
import icecube.daq.payload.ByteBufferCache;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
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

class MockInputChannel
    extends InputChannel
{
    private IByteBufferCache bufMgr;

    MockInputChannel(InputChannelParent parent, SelectableChannel channel,
                     IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        super(parent, channel, bufMgr, bufSize);

        this.bufMgr = bufMgr;
    }

    public void pushPayload(ByteBuffer buf)
    {
        bufMgr.returnBuffer(buf);
    }
}

class MockReader
    extends PayloadReader
{
    MockReader(String name)
    {
        super(name);
    }

    public InputChannel createChannel(SelectableChannel channel,
                                      IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        return new MockInputChannel(this, channel, bufMgr, bufSize);
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
                //logger.debug("Generated " + nevt + " events.");
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
            logger.error("Generator error", intx);
            return;
        }
        catch (IOException iox)
        {
            logger.error("Generator error", iox);
            return;
        }
    }
}

public class PieStressor 
{
    private static final int NUM_INPUTS = 20;
    private static final int NUM_REPS = 1000;

    private ByteBufferCache     cache;
    private boolean             printChannelStates;
    
    private PieStressor()
    {
        cache  = new ByteBufferCache(256, 50000000L, 50000000L);
        printChannelStates = false;
    }
    
    private static final void report(String title, long[] prevRdr, Long[] brec)
    {
        System.out.print(title + ": ");
        for (int j = 0; j < brec.length; j++)
        {
            long val = brec[j].longValue();
            System.out.format("%5d ", val - prevRdr[j]);
            prevRdr[j] = val;
        }
        System.out.println();
    }

    private void runTest(boolean tryEngine, boolean tryReader, double rate) throws Exception
    {
        System.out.println("running test");

        DevNullInputEngine engine;
        if (!tryEngine) {
            engine = null;
        } else {
            engine = new DevNullInputEngine(cache);
            engine.start();
            engine.startServer(cache);
        }

        MockReader reader;
        if (!tryReader) {
            reader = null;
        } else {
            reader = new MockReader("MockRdr");
            reader.start();
            reader.startServer(cache);
        }

        ArrayList<Paygen> genList = new ArrayList<Paygen>();

        for (int i = 0; i < NUM_INPUTS; i++)
        {
            Paygen pagan;

            if (tryEngine) {
                pagan = new Paygen(engine.getServerPort(), rate);
                pagan.setName("engGen#" + i);
                genList.add(pagan);
            }

            if (tryReader) {
                pagan = new Paygen(reader.getServerPort(), rate);
                pagan.setName("rdrGen#" + i);
                genList.add(pagan);
            }
        }

        Thread.sleep(1000);
        if (tryEngine) engine.startProcessing();
        if (tryReader) reader.startProcessing();

        for (Paygen pg : genList) {
            pg.start();
        }

        long[] prevEng = new long[NUM_INPUTS];
        long[] prevRdr = new long[NUM_INPUTS];
        for (int i = 0; i < NUM_INPUTS; i++) {
            prevEng[i] = 0;
            prevRdr[i] = 0;
        }

        for (int i = 0; i < NUM_REPS; i++)
        {
            Thread.sleep(1000);

            if (tryEngine) report("Eng", prevEng, engine.getRecordsReceived());
            if (tryReader) report("Rdr", prevRdr, reader.getRecordsReceived());

            if (printChannelStates && tryEngine)
            {
                String[] states = engine.getPresentChannelStates();
                for (int j = 0; j < states.length; j++)
                {
                   System.out.format("%10s ", states[j]);
                }
                System.out.println();
            }

            // empty the byte buffers
            if (tryEngine) engine.recycleBuffers();
        }

        System.out.println("stopping engine.");
        if (tryEngine) engine.destroyProcessor();
        if (tryReader) reader.destroyProcessor();
    }
    
    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();
        BasicConfigurator.configure(new MockAppender(Level.WARN));
        Logger.getRootLogger().setLevel(Level.INFO);

        boolean tryEngine = true;
        boolean tryReader = false;

        if (args.length > 0 && args[0].length() > 0) {
            tryEngine = false;
            tryReader = false;

            switch (args[0].charAt(0)) {
            case 'b':
            case 'B':
                tryEngine = true;
                tryReader = true;
                break;
            case 'e':
            case 'E':
            case 'i':
            case 'I':
                tryEngine = true;
                break;
            case 'r':
            case 'R':
                tryReader = true;
                break;
            default:
                System.err.println("Unknown argument '" + args[0] + "'");
                System.exit(1);
                break;
            }
        }

        double rate = 1000.0;
        if (args.length > 1) rate = Double.parseDouble(args[1]);
        System.out.println("Rate is " + rate + " Hz.");
        PieStressor test = new PieStressor();
        test.runTest(tryEngine, tryReader, rate);
    }
}
