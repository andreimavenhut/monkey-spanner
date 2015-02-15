package spanner.monkey.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class UDFGzuncompress extends UDF
{
    private final transient Text result = new Text();
    private final transient Inflater decompressed = new Inflater();
    static final Log LOG = LogFactory.getLog(UDFGzuncompress.class.getName());

    public final static int BUFFER_SIZE = 4096;

    private Text evaluate(byte[] b)
    {
        decompressed.setInput(b);
        byte[] buffer = new byte[b.length];
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            while (!decompressed.finished() && decompressed.inflate(buffer) != 0) {
                outputStream.write(buffer);
            }
        }
        catch (DataFormatException | IOException e) {
            LOG.warn("Exception when gzuncompress binary data", e);
            decompressed.reset();
            return null;
        }

        decompressed.reset();
        result.set(outputStream.toByteArray());
        return result;

    }


    public Text evaluate(BytesWritable b)
    {
        if (b == null) {
            return null;
        }

        return evaluate(b.getBytes());
    }

    private static byte[] hexStringToByteArray(String s)
    {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public Text evaluate(Text t)
    {
        if (t == null) {
            return null;
        }
        return evaluate(hexStringToByteArray(t.toString()));
    }
}
