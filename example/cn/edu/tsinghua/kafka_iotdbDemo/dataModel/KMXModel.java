package cn.edu.tsinghua.kafka_iotdbDemo.dataModel;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/11/15 0015.
 */
public class KMXModel {
    public String fieldGroupId;
    public List<KMXField> IdFields;
    public List<KMXField> nonIdFields;
    public String sampleTime;

    public KMXModel(){}

    public KMXModel(String jsonString) {
        this.IdFields = new ArrayList<>();
        this.nonIdFields = new ArrayList<>();

        JSONObject jsonObject = new JSONObject(jsonString);
        this.fieldGroupId = jsonObject.getString("fieldGroupId");
        parseFields(jsonObject.getJSONArray("fields"));
        parseTime(jsonObject.getJSONObject("sampleTime"));
    }

    private void parseFields(JSONArray array) {
        for(int i = 0; i < array.length(); i++) {
            KMXField field = new KMXField(array.getJSONObject(i));
            if(field.isIdField) {
                IdFields.add(field);
            } else {
                nonIdFields.add(field);
            }
        }
    }

    private void parseTime(JSONObject object) {
        if(object.has("timestamp")) {
            sampleTime = object.get("timestamp").toString();
        } else {
            sampleTime = object.get("iso").toString();
        }
    }
}

class KMXField implements Comparable<KMXField>{
    public boolean isIdField;
    public String fieldValue;
    public String fieldId;

    public KMXField(){}

    public KMXField(JSONObject object) {
        this.isIdField = object.has("isIdField") ? object.getBoolean("isIdField") : false;
        this.fieldValue = object.get("fieldValue").toString();
        this.fieldId = object.getString("fieldId");
    }

    @Override
    /**
     * first by Id, then by value
     */
    public int compareTo(KMXField o) {
        int IdComparison = this.fieldId.compareTo(o.fieldId);
        if(IdComparison != 0) {
            return IdComparison;
        }
        return this.fieldValue.compareTo(o.fieldValue);
    }
}
