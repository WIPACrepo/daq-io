package icecube.daq.io;

import icecube.daq.payload.IDOMID;
import icecube.daq.payload.IEventHitRecord;
import icecube.daq.payload.IEventPayload;
import icecube.daq.payload.IHitData;
import icecube.daq.payload.IReadoutDataPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.impl.DOMID;
import icecube.daq.payload.impl.EventPayload_v5;
import icecube.daq.payload.impl.EventPayload_v6;
import icecube.daq.payload.impl.PayloadFactory;
import icecube.daq.util.DOMRegistry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;

public class ConvertEvents
{
    private HashMap<IDOMID,Short> DOM2CHAN;

    ConvertEvents()
        throws Exception
    {
        loadChannelIds();
    }

    private List<IEventHitRecord> extractHits(IEventPayload evt)
    {
        List rdpList = evt.getReadoutDataPayloads();
        if (rdpList == null) {
            throw new Error("Readout data payload list is null for " + evt);
        }

        List<IEventHitRecord> hitRecList = new ArrayList<IEventHitRecord>();
        for (Object obj: rdpList) {
            IReadoutDataPayload rdp = (IReadoutDataPayload) obj;

            try {
                rdp.loadPayload();
            } catch (Exception ex) {
                System.err.println("Cannot load readout data payload " + rdp);
                ex.printStackTrace();
                continue;
            }

            List dataList = rdp.getDataPayloads();
            if (dataList == null) {
                throw new Error("Data list is null for " + rdp + " from " +
                                evt);
            }

            for (Object o2 : dataList) {
                IHitData hit = (IHitData) o2;
                short chanId = getChannelId(hit.getDOMID());
                IEventHitRecord rec;
                try {
                    rec = hit.getEventHitRecord(chanId);
                } catch (PayloadException pe) {
                    System.err.println("Cannot get record for hit " + hit);
                    pe.printStackTrace();
                    continue;
                }
                hitRecList.add(rec);
            }
        }

        return hitRecList;
    }

    private short getChannelId(IDOMID domId)
    {
        if (!DOM2CHAN.containsKey(domId)) {
            return -1;
        }

        return DOM2CHAN.get(domId);
    }

    private void loadChannelIds()
        throws Exception
    {
        String configDir = getClass().getResource("/config/").getPath();
        DOMRegistry reg = DOMRegistry.loadRegistry(configDir);

        DOM2CHAN = new HashMap<IDOMID,Short>();
        for (String mbStr : reg.keys()) {
            IDOMID mbId;
            try {
                mbId = new DOMID(Long.valueOf(mbStr, 16));
            } catch (NumberFormatException nfe) {
                System.err.println("Bad mainboard ID \"" + mbStr + "\"");
                continue;
            }

            DOM2CHAN.put(mbId, (short) reg.getChannelId(mbStr));
        }

        System.err.println("Loaded " + DOM2CHAN.size() + " DOMs");
    }

    void run(ArrayList<File> fileList, int version, boolean readOnly,
             boolean testBuffer)
        throws IOException, PayloadException
    {
        int numFailed = 0;

        PayloadFactory factory = new PayloadFactory(null);

        for (File f : fileList) {
            PayloadFileReader rdr = new PayloadFileReader(f);

            BufferWriter out;
            if (readOnly) {
                out = null;
            } else {
                out = new BufferWriter(f.getName() + ".v" + version);
            }

            System.err.println(f.getName());

            while (rdr.hasNext()) {
                IEventPayload evt = (IEventPayload) rdr.next();

                try {
                    evt.loadPayload();
                } catch (Exception ex) {
                    System.err.println("Cannot load event " + evt);
                    ex.printStackTrace();
                    continue;
                }

                if (readOnly) {
                    continue;
                }

                EventPayload_v5 ev5;
                if (version == 5) {
                    ev5 =
                        new EventPayload_v5(evt.getEventUID(),
                                            evt.getFirstTimeUTC(),
                                            evt.getLastTimeUTC(), evt.getYear(),
                                            evt.getRunNumber(),
                                            evt.getSubrunNumber(),
                                            evt.getTriggerRequestPayload(),
                                            extractHits(evt));
                } else if (version == 6) {
                    ev5 =
                        new EventPayload_v6(evt.getEventUID(),
                                            evt.getFirstTimeUTC(),
                                            evt.getLastTimeUTC(), evt.getYear(),
                                            evt.getRunNumber(),
                                            evt.getSubrunNumber(),
                                            evt.getTriggerRequestPayload(),
                                            extractHits(evt));
                } else {
                    throw new Error("Unknown event version #" + version);
                }

                ByteBuffer buf = ByteBuffer.allocate(ev5.getPayloadLength());
                try {
                    ev5.writePayload(false, 0, buf);
                } catch (IOException ioe) {
                    System.err.println("Failed to write " + ev5);
                    //ioe.printStackTrace();
                    numFailed++;
                    continue;
                }

                if (testBuffer) {
                    factory.getPayload(buf, 0);
                }

                out.write(buf);
            }

            if (out != null) {
                out.close();
            }
            rdr.close();
        }

        if (numFailed > 0) {
            System.err.println("Failed to write " + numFailed + " events");
        }
    }

    public static final void main(String[] args)
        throws Exception
    {
        ConsoleAppender appender = new ConsoleAppender(new PatternLayout());

        LogManager.resetConfiguration();
        LogManager.getRootLogger().addAppender(appender);

        int version = 5;
        boolean readOnly = false;
        boolean testBuffer = false;

        ArrayList<File> fileList = new ArrayList<File>();

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                switch (args[i].charAt(1)) {
                case '5':
                    version = 5;
                    break;
                case '6':
                    version = 6;
                    break;
                case 'r':
                    readOnly = true;
                    break;
                case 't':
                    testBuffer = true;
                    break;
                default:
                    throw new Error("Unknown argument \"" + args[i] + "\"");
                }
                continue;
            }

            File f = new File(args[i]);
            if (!f.exists()) {
                System.err.println("File \"" + args[i] + "\" does not exist");
                continue;
            }

            fileList.add(f);
        }

        ConvertEvents cvt = new ConvertEvents();
        cvt.run(fileList, version, readOnly, testBuffer);
    }
}
