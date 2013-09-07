package spanner.monkey.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

public class UDFURLDecode extends UDF {
    private static List<Charset> decodeCharsets;

    static {
        decodeCharsets = new ArrayList<Charset>();
        decodeCharsets.add(Charset.forName("UTF-8"));
        decodeCharsets.add(Charset.forName("EUC-JP"));
        decodeCharsets.add(Charset.forName("ISO-2022-JP"));
        decodeCharsets.add(Charset.forName("windows-31j"));
    }


    @Description(name = "url_decode",
            value = "_FUNC_(str) - decode a url-encoded string")
    public String evaluate(String str) throws UnsupportedEncodingException {
        if (str == null) {
            return null;
        }

        byte[] b = null;
        try {
            b = URLDecoder.decode(str, "ISO-8859-1").getBytes("ISO-8859-1");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        for (Charset charset : decodeCharsets) {
            CharsetDecoder decoder = charset.newDecoder();
            try {
                return decoder.decode(ByteBuffer.wrap(b)).toString();
            } catch (CharacterCodingException e) {
                continue;
            }
        }

        return str;
    }

    @Description(name = "url_decode",
            value = "_FUNC_(str, encs) - decode a url-encoded string with the specified character set")
    public String evaluate(String str, String encs) {
        if (str == null || encs == null) {
            return null;
        }

        try {
            return URLDecoder.decode(str, encs);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return null;

    }
}
