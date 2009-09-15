package icecube.daq.io;

import icecube.daq.payload.impl.BasePayload;
import icecube.daq.payload.ILoadablePayload;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;

public class PayloadDumper
{
    private static Log LOG = LogFactory.getLog(PayloadDumper.class);

    public static final void main(String[] args)
        throws IOException
    {
        BasicConfigurator.configure();

        boolean dumpHex = false;

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                if (args[i].charAt(1) == 'h') {
                    dumpHex = true;
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

                try {
                    payload.loadPayload();
                } catch (Exception ex) {
                    LOG.error("Couldn't load payload");
                    ex.printStackTrace();
                    continue;
                }

                System.out.println(payload.toString());
            }
        }
    }
}
