package icecube.daq.io;

import icecube.daq.payload.impl.BasePayload;
import icecube.daq.payload.impl.TriggerRequest;
import icecube.daq.payload.IEventHitRecord;
import icecube.daq.payload.IEventPayload;
import icecube.daq.payload.IEventTriggerRecord;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.PayloadRegistry;
import icecube.daq.payload.SourceIdRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;

public class PayloadDumper
{
    private static Log LOG = LogFactory.getLog(PayloadDumper.class);

    public static void dumpComplex(ILoadablePayload payload)
    {
        try {
            payload.loadPayload();
        } catch (Exception ex) {
            LOG.error("Couldn't load payload", ex);
            return;
        }

        switch (payload.getPayloadType()) {
        case PayloadRegistry.PAYLOAD_ID_EVENT_V5:
            dumpEvent((IEventPayload) payload, false);
            break;
        default:
            System.out.println("Not handling payload type " +
                               payload.getPayloadType());
            break;
        }
    }

    public static void dumpEvent(IEventPayload evt, boolean showHitRecords)
    {
        System.out.println(evt.toString());

        try {
            boolean printHdr = true;
            for (IEventTriggerRecord trigRec : evt.getTriggerRecords()) {
                if (!printHdr) {
                    System.out.println("-- Trigger Records");
                    printHdr = false;
                }

                ArrayList<IEventHitRecord> hitRecList =
                    new ArrayList<IEventHitRecord>();
                for (IEventHitRecord hitRec : evt.getHitRecords()) {
                    hitRecList.add(hitRec);
                }

                dumpTriggerRecord(trigRec, hitRecList, evt.getUTCTime());
            }
        } catch (Error err) {
            System.out.println("-- No trigger records");
        }

        try {
            boolean printHdr = true;
            for (Object obj : evt.getReadoutDataPayloads()) {
                if (!printHdr) {
                    System.out.println("-- Readout Data");
                    printHdr = false;
                }

                dumpComplex((ILoadablePayload) obj);
            }
        } catch (Error err) {
            System.out.println("-- No readout data");
        }

        if (showHitRecords) {
            try {
                boolean printHdr = true;
                for (IEventHitRecord hitRec : evt.getHitRecords()) {
                    if (!printHdr) {
                        System.out.println("-- Hit Records");
                        printHdr = false;
                    }

                    System.out.println(hitRec.toString());
                }
            } catch (Error err) {
                System.out.println("-- No hit records");
            }
        }
    }

    public static void dumpSimple(ILoadablePayload payload)
    {
        try {
            payload.loadPayload();
        } catch (Exception ex) {
            LOG.error("Couldn't load payload", ex);
            return;
        }

        System.out.println(payload.toString());
    }

    public static void dumpTriggerRecord(IEventTriggerRecord trigRec,
                                         List<IEventHitRecord> fullList,
                                         long evtStartTime)
    {
        final String trigName =
            TriggerRequest.getTriggerName(trigRec.getType(),
                                          trigRec.getConfigID(), 0);

        final String srcName = getSourceName(trigRec.getSourceID());

        String tstr =
            String.format(" - trig %s cfg %d %s start %.1f dur %.1f",
                          trigName, trigRec.getConfigID(), srcName,
                          (trigRec.getFirstTime() - evtStartTime) * 0.1,
                          (trigRec.getLastTime() -
                           trigRec.getFirstTime()) * 0.1);
        System.out.println(tstr);

        int[] idxList = trigRec.getHitRecordIndexList();

        long prevTime = 0L;
        for (int i = 0; i < idxList.length; i++) {
            String hstr;
            if (idxList[i] < 0 || idxList[i] > fullList.size()) {
                hstr = String.format("!! bad hit index #%d !!", idxList[i]);
            } else {
                IEventHitRecord hit = fullList.get(idxList[i]);

                String mark;
                if (prevTime < hit.getHitTime()) {
                    mark = "";
                } else {
                    mark = " !!!";
                }

                hstr = String.format("#%d chan %d time %.1f%s", idxList[i],
                                     hit.getChannelID(),
                                     (hit.getHitTime() - evtStartTime) * 0.1,
                                     mark);

                prevTime = hit.getHitTime();
            }

            System.out.println("  - hit " + hstr);
        }
    }

    private static final String getSourceName(int srcId)
    {
        switch (srcId) {
        case SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID:
            return "IceTop";
        case SourceIdRegistry.INICE_TRIGGER_SOURCE_ID:
            return "InIce";
        case SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID:
            return "Global";
        default:
            break;
        }

        return "??Src#" + srcId + "??";
    }

    public static final void main(String[] args)
        throws IOException
    {
        BasicConfigurator.configure();

        boolean dumpHex = false;
        boolean dumpFull = false;

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                if (args[i].charAt(1) == 'h') {
                    dumpHex = true;
                } else if (args[i].charAt(1) == 'f') {
                    dumpFull = true;
                } else {
                    throw new Error("Unknown option \"" + args[i] + "\"");
                }

                continue;
            }

            DAQFileReader rdr = new PayloadFileReader(args[i]);
            for (Object obj : rdr) {
                ILoadablePayload payload = (ILoadablePayload) obj;

                if (dumpHex) {
                    ByteBuffer buf = payload.getPayloadBacking();
                    if (buf != null) {
                        System.err.println(BasePayload.toHexString(buf, 0));
                    }
                }

                if (!dumpFull) {
                    dumpSimple(payload);
                } else {
                    dumpComplex(payload);
                }
            }
        }
    }
}
