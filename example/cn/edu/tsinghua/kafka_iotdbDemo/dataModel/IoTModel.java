package cn.edu.tsinghua.kafka_iotdbDemo.dataModel;

/**
 * Created by Administrator on 2017/11/15 0015.
 */
public class IoTModel {
    public String deltaObj;
    public String measurement;
    public String value;
    public String sampleTime;

    public  IoTModel() {}

    public String getFullPath() {
        return this.deltaObj + "." + this.measurement;
    }
}
