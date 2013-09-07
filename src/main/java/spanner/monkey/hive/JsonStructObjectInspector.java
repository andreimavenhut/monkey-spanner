package spanner.monkey.hive;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JsonStructObjectInspector extends StandardStructObjectInspector {

    static HashMap<ArrayList<Object>, JsonStructObjectInspector>
            cachedStandardStructObjectInspector =
            new HashMap<ArrayList<Object>, JsonStructObjectInspector>();

    // factory method
    public static JsonStructObjectInspector getJsonStructObjectInspector(
            List<String> structFieldNames,
            List<ObjectInspector> structFieldObjectInspectors
    ) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(structFieldNames);
        signature.add(structFieldObjectInspectors);

        JsonStructObjectInspector result = cachedStandardStructObjectInspector.get(signature);
        if (result == null) {
            result = new JsonStructObjectInspector(structFieldNames,
                    structFieldObjectInspectors);
            cachedStandardStructObjectInspector.put(signature, result);
        }
        return result;
    }


    public JsonStructObjectInspector(List<String> structFieldNames,
                                     List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        if (data == null) {
            return null;
        }
        JSONObject obj = null;

        if (data instanceof JSONObject) {
            return getStructFieldDataFromJsonObject((JSONObject) data, fieldRef);
        }
        if (data instanceof List) {
            return getStructFieldDataFromList((List) data, fieldRef);
        } else {
            throw new Error("Data is not JSONObject  but " + data.getClass().getCanonicalName() +
                    " with value " + data.toString());
        }

    }

    @Override
    public Object create() {
        return new JSONObject();
    }

    public Object getStructFieldDataFromList(List data, StructField fieldRef) {
        int idx = fields.indexOf(fieldRef);
        if (idx < 0 || idx >= data.size()) {
            return null;
        } else {
            return data.get(idx);
        }
    }

    public Object getStructFieldDataFromJsonObject(JSONObject data, StructField fieldRef) {
        if (data == null) {
            return null;
        }

        MyField f = (MyField) fieldRef;

        int fieldID = f.getFieldID();
        assert (fieldID >= 0 && fieldID < fields.size());

        if (data.containsKey(getJsonField(fieldRef))) {
            return data.get(getJsonField(fieldRef));
        } else {
            return null;
        }
    }


    static List<Object> values = new ArrayList<Object>();


    protected String getJsonField(StructField fr) {
        return fr.getFieldName();
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object o) {
        JSONObject jobj = (JSONObject) o;
        values.clear();

        for (int i = 0; i < fields.size(); i++) {
            if (jobj.containsKey(getJsonField(fields.get(i)))) {
                values.add(getStructFieldData(o, fields.get(i)));
            } else {
                values.add(null);
            }
        }
        return values;
    }


    @Override
    public Object setStructFieldData(Object struct, StructField field,
                                     Object fieldValue) {
        JSONObject a = (JSONObject) struct;
        MyField myField = (MyField) field;
        a.put(myField.getFieldName(), fieldValue);
        return a;
    }
}

