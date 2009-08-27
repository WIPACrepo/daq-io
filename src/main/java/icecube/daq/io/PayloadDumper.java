package icecube.daq.io;

import icecube.daq.payload.ILoadablePayload;

import java.io.IOException;

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

        DAQFileReader rdr = new PayloadFileReader(args[0]);
        for (Object obj : rdr) {
            ILoadablePayload payload = (ILoadablePayload) obj;

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
