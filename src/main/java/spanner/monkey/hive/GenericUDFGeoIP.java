package spanner.monkey.hive;

import com.google.common.base.Strings;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Location;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@Description(
        name = "geoip",
        value = "_FUNC_(ip,property,database) - GEO IP lookup\n" +
                "possible properties: json,country,country_code,city,location,timezone,postal_code,subdivision,subdivision_code"
)
public class GenericUDFGeoIP extends GenericUDF {
    private String ipString = null;
    private Long ipLong = null;
    private String property;
    private String database;
    transient private DatabaseReader reader;
    transient StringBuilder sb = new StringBuilder();

    private static final String JSON = "JSON";
    private static final String COUNTRY = "COUNTRY";
    private static final String COUNTRY_CODE = "COUNTRY_CODE";
    private static final String CITY = "CITY";
    private static final String LOCATION = "LOCATION";
    private static final String TIMEZONE = "TIMEZONE";
    private static final String POSTAL_CODE = "POSTAL_CODE";
    private static final String SUBDIVISION = "SUBDIVISION";
    private static final String SUBDIVISION_CODE = "SUBDIVISION_CODE";

    private static final Set<String> COUNTRY_PROPERTIES =
            new CopyOnWriteArraySet<String>(Arrays.asList(
                    new String[]{COUNTRY, COUNTRY_CODE}));

    private static final Set<String> CITY_PROPERTIES =
            new CopyOnWriteArraySet<String>(Arrays.asList(
                    new String[]{JSON, COUNTRY, COUNTRY_CODE,
                            CITY, LOCATION, TIMEZONE, SUBDIVISION, SUBDIVISION_CODE}));

    PrimitiveObjectInspector[] argumentOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        argumentOIs = new PrimitiveObjectInspector[arguments.length];

        if (arguments.length != 3) {
            throw new UDFArgumentLengthException(
                    "The function GenericUDFGeoIP( 'input', 'resultfield', 'datafile' ) "
                            + " accepts 3 arguments.");
        }

        if (!(arguments[0] instanceof StringObjectInspector) && !(arguments[0] instanceof LongObjectInspector)) {
            throw new UDFArgumentTypeException(0,
                    "The first 3 parameters of GenericUDFGeoIP('input', 'resultfield', 'datafile')"
                            + " should be string.");
        }
        argumentOIs[0] = (PrimitiveObjectInspector) arguments[0];

        for (int i = 1; i < arguments.length; i++) {
            if (!(arguments[i] instanceof StringObjectInspector)) {
                throw new UDFArgumentTypeException(i,
                        "The first 3 parameters of GenericUDFGeoIP('input', 'resultfield', 'datafile')"
                                + " should be string.");
            }
            argumentOIs[i] = (StringObjectInspector) arguments[i];
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    private String longToIp(long ip) {
        StringBuilder sb = new StringBuilder(15);
        for (int i = 0; i < 4; i++) {
            sb.insert(0, Long.toString(ip & 0xff));

            if (i < 3) {
                sb.insert(0, '.');
            }
            ip = ip >> 8;
        }
        return sb.toString();
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        if (argumentOIs[0] instanceof LongObjectInspector) {
            this.ipLong = ((LongObjectInspector) argumentOIs[0]).get(arguments[0].get());
        } else {
            this.ipString = ((StringObjectInspector) argumentOIs[0]).getPrimitiveJavaObject(arguments[0].get());
        }
        this.property = ((StringObjectInspector) argumentOIs[1]).getPrimitiveJavaObject(arguments[1].get());

        if (Strings.isNullOrEmpty(this.property)) {
            return null;
        } else {
            this.property = this.property.toUpperCase();
        }

        if (reader == null) {
            if (argumentOIs.length == 3) {
                this.database = ((StringObjectInspector) argumentOIs[1]).getPrimitiveJavaObject(arguments[2].get());
                File f = new File(database);
                if (!f.exists()) {
                    throw new HiveException(database + " does not exist");
                }
                try {
                    reader = new DatabaseReader.Builder(f).build();
                } catch (IOException ex) {
                    throw new HiveException(ex);
                }
            }
        }

        sb.setLength(0);

        try {
            CityResponse cityResponse = (ipString != null) ? reader.city(InetAddress.getByName(ipString)) :
                    reader.city(InetAddress.getByName(longToIp(ipLong)));
            boolean first = true;
            for (String p : property.split(",")) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                if (CITY_PROPERTIES.contains(p)) {
                    if (p.equals(JSON)) {
                        sb.append(cityResponse.toJson());
                    } else if (p.equals(COUNTRY)) {
                        sb.append(cityResponse.getCountry().getName());
                    } else if (p.equals(COUNTRY_CODE)) {
                        sb.append(cityResponse.getCountry().getIsoCode());
                    } else if (p.equals(CITY)) {
                        sb.append(cityResponse.getCity().getName());
                    } else if (p.equals(LOCATION)) {
                        Location location = cityResponse.getLocation();
                        sb.append(location.getLatitude() + ":" + location.getLongitude());
                    } else if (p.equals(POSTAL_CODE)) {
                        sb.append(cityResponse.getPostal().getCode());
                    } else if (p.equals(TIMEZONE)) {
                        sb.append(cityResponse.getLocation().getTimeZone());
                    } else if (p.equals(SUBDIVISION)) {
                        sb.append(cityResponse.getMostSpecificSubdivision().getName());
                    } else if (p.equals(SUBDIVISION_CODE)) {
                        sb.append(cityResponse.getMostSpecificSubdivision().getIsoCode());
                    }
                } else {
                    sb.append("null");
                }
            }

        } catch (IOException | GeoIp2Exception e) {
            return null;
        }

        return sb.toString();
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 3);
        return "GenericUDFGeoIP ( " + children[0] + ", " + children[1] + ", " + children[2] + " )";
    }
}
