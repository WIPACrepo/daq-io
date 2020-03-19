package icecube.daq.io;

import icecube.daq.payload.impl.BasePayload;
import icecube.daq.payload.impl.TriggerRequest;
import icecube.daq.payload.IEventHitRecord;
import icecube.daq.payload.IEventPayload;
import icecube.daq.payload.IEventTriggerRecord;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IReadoutRequest;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.PayloadChecker;
import icecube.daq.payload.PayloadRegistry;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.util.LocatePDAQ;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class PayloadDumper
{
    private static final Logger LOG = Logger.getLogger(PayloadDumper.class);

    private static final String INDENT = "   ";

    public static void dumpComplex(IPayload payload)
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
        case PayloadRegistry.PAYLOAD_ID_TRIGGER_REQUEST:
            dumpTriggerRequest((ITriggerRequestPayload) payload, INDENT);
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

        boolean foundTRec = false;
        boolean foundRdout = false;
        boolean foundErr = false;

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

                dumpTriggerRecord(trigRec, hitRecList, evt.getUTCTime(),
                                  INDENT);
                foundTRec = true;
            }
        } catch (Error err) {
            foundErr = true;
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
                foundErr = true;
            }
        }
    }

    public static void dumpReadoutRequest(IReadoutRequest rReq, String indent)
    {
        System.out.println(indent + rReq);

        List elems = rReq.getReadoutRequestElements();
        if (elems != null) {
            final String i2 = indent + INDENT;
            for (Object obj : elems) {
                System.out.println(i2 + obj);
            }
        }
    }

    public static void dumpSimple(IPayload payload)
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
                                         long evtStartTime, String indent)
    {
        final String trigName =
            TriggerRequest.getTriggerName(trigRec.getType(),
                                          trigRec.getConfigID(), 0);

        final String srcName = getSourceName(trigRec.getSourceID());

        String tstr =
            String.format("%strig %s cfg %d %s start %.1f dur %.1f",
                          indent, trigName, trigRec.getConfigID(), srcName,
                          (trigRec.getFirstTime() - evtStartTime) * 0.1,
                          (trigRec.getLastTime() -
                           trigRec.getFirstTime()) * 0.1);
        System.out.println(tstr);

        int[] idxList = trigRec.getHitRecordIndexList();

        final String indent2 = indent + INDENT;

        long prevTime = 0L;
        for (int i = 0; i < idxList.length; i++) {
            String hstr;
            if (idxList[i] < 0 || idxList[i] > fullList.size()) {
                hstr = String.format("%s!! bad hit index #%d !!", indent2,
                                     idxList[i]);
            } else {
                IEventHitRecord hit = fullList.get(idxList[i]);

                String mark;
                if (prevTime < hit.getHitTime()) {
                    mark = "";
                } else {
                    mark = " !!!";
                }

                hstr = String.format("%shit #%d chan %d time %.1f%s",
                                     indent2, idxList[i], hit.getChannelID(),
                                     (hit.getHitTime() - evtStartTime) * 0.1,
                                     mark);

                hstr = indent2 + hit.toString();
                prevTime = hit.getHitTime();
            }

            System.out.println(hstr);
        }
    }

    public static void dumpTriggerRequest(ITriggerRequestPayload trigReq,
                                          String indent)
    {
        System.out.println(indent + trigReq);

        IReadoutRequest rReq = trigReq.getReadoutRequest();
        if (rReq != null) {
            dumpReadoutRequest(rReq, indent + INDENT);
        } else {
            System.out.println(indent + "--- No ReadoutRequest data");
        }

        Collection<IPayload> compList;
        try {
            compList = trigReq.getPayloads();
        } catch (Exception ex) {
            LOG.error("Couldn't get composite payloads", ex);
            return;
        }

        for (IPayload pay : compList) {
            if (pay instanceof ITriggerRequestPayload) {
                dumpTriggerRequest((ITriggerRequestPayload) pay,
                                   indent + INDENT);
            } else {
                System.out.println(indent + INDENT + pay);
            }
        }
    }

    private static String getSourceName(int srcId)
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

        boolean usage = false;

        boolean dumpHex = false;
        boolean dumpFull = false;
        boolean summarize = false;
        boolean validate = false;

        boolean getMax = false;
        long maxPayloads = Long.MAX_VALUE;

        boolean getCfg = false;
        String runCfgName = null;

        boolean getCfgDir = false;
        File configDir = null;

        ArrayList<File> files = new ArrayList<File>();

        for (int i = 0; i < args.length; i++) {
            if (getMax) {
                try {
                    long tmp = Long.parseLong(args[i]);
                    maxPayloads = tmp;
                } catch (NumberFormatException nfe) {
                    System.err.println("Bad number of payloads \"" + args[i] +
                                       "\"");
                    usage = true;
                }

                getMax = false;
                continue;
            }

            if (getCfg) {
                runCfgName = args[i];
                getCfg = false;
                continue;
            }

            if (getCfgDir) {
                File tmpCfgDir = new File(args[i]);
                if (tmpCfgDir.isDirectory()) {
                    configDir = tmpCfgDir;
                } else {
                    System.err.println("Bad config directory \"" +
                                       tmpCfgDir + "\"");
                    usage = true;
                }

                getCfgDir = false;
                continue;
            }

            if (args[i].length() > 1 && args[i].charAt(0) == '-') {
                switch(args[i].charAt(1)) {
                case 'D':
                    if (args[i].length() == 2) {
                        getCfgDir = true;
                    } else {
                        File tmpCfgDir = new File(args[i].substring(2));
                        if (tmpCfgDir.isDirectory()) {
                            configDir = tmpCfgDir;
                        } else {
                            System.err.println("Bad config directory \"" +
                                               tmpCfgDir + "\"");
                            usage = true;
                        }
                    }
                    break;
                case 'S':
                    summarize = true;
                    break;
                case 'c':
                    if (args[i].length() == 2) {
                        getCfg = true;
                    } else {
                        runCfgName = args[i].substring(2);
                    }
                    break;
                case 'f':
                    dumpFull = true;
                    break;
                case 'h':
                    dumpHex = true;
                    break;
                case 'n':
                    if (args[i].length() == 2) {
                        getMax = true;
                    } else {
                        try {
                            long tmp = Long.parseLong(args[i].substring(2));
                            maxPayloads = tmp;
                        } catch (NumberFormatException nfe) {
                            System.err.println("Bad number of payloads \"" +
                                               args[i].substring(2) + "\"");
                            usage = true;
                        }
                    }
                    break;
                case 'v':
                    validate = true;
                    break;
                default:
                    System.err.println("Bad option \"" + args[i] +
                                       "\"; valid options are -h(ex) and" +
                                       " -f(ull)");
                    usage = true;
                    break;
                }

                continue;
            }

            File f = new File(args[i]);
            if (f.exists()) {
                files.add(f);
            } else {
                System.err.println("Ignoring nonexistent file \"" + f + "\"");
                usage = true;
            }
        }

        if (runCfgName != null) {
            if (configDir == null) {
                try {
                    configDir = LocatePDAQ.findConfigDirectory();
                } catch (IllegalArgumentException iae) {
                    System.err.println("Cannot find configuration directory");
                    System.err.println("Please specify config directory (-D)");
                    configDir = null;
                    usage = true;
                }
            }

            if (configDir != null) {
                PayloadChecker.configure(configDir, runCfgName);
            }
        }

        if (usage) {
            System.err.print("Usage: ");
            System.err.print("java PayloadDumper");
            System.err.print(" [-D configDir]");
            System.err.print(" [-S(ummarize)]");
            System.err.print(" [-c runConfigName)]");
            System.err.print(" [-f(ullDump)]");
            System.err.print(" [-h(exDump)]");
            System.err.print(" [-n numToDump]");
            System.err.print(" [-v(alidate)]");
            System.err.print(" payloadFile ...");
            System.err.println();
            System.exit(1);
        }

        for (File f : files) {
            long numPayloads = 0;
            long firstTime = Long.MIN_VALUE;
            long lastTime = Long.MIN_VALUE;

            PayloadFileReader rdr = new PayloadFileReader(f);
            for (Object obj : rdr) {
                IPayload payload = (IPayload) obj;

                if (summarize) {
                    if (firstTime == Long.MIN_VALUE) {
                        firstTime = payload.getUTCTime();
                    }
                    lastTime = payload.getUTCTime();
                } else {
                    if (dumpHex) {
                        ByteBuffer buf = payload.getPayloadBacking();
                        if (buf == null) {
                            System.err.println("Cannot get payload backing" +
                                               " for " + payload);
                        } else {
                            String hexStr = BasePayload.toHexString(buf, 0);
                            System.out.println(hexStr);
                        }
                    }

                    if (!dumpFull) {
                        dumpSimple(payload);
                    } else {
                        dumpComplex(payload);
                    }
                }

                if (validate &&
                    PayloadChecker.validatePayload(payload, true))
                {
                    System.err.printf("***** Payload#%d was not valid\n",
                                      numPayloads);
                }

                // exit loop if we've seen the requested number of payloads
                if (++numPayloads >= maxPayloads) {
                    break;
                }
            }

            if (summarize) {
                if (firstTime == Long.MIN_VALUE) {
                    System.out.printf("%s: NO DATA\n", f);
                } else {
                    System.out.printf("%s: %d payloads, %d-%d\n", f,
                                      numPayloads, firstTime, lastTime);
                }
            }
        }
    }
}
