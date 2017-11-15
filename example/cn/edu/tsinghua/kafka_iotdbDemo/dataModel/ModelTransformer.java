package cn.edu.tsinghua.kafka_iotdbDemo.dataModel;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/11/15 0015.
 */
public class ModelTransformer {
    /**
     * The transformation is as follow :
     * use the sorted IdField as path levels to form the deltaObject
     * use the nonIdField's Id as the measurement, meaning each nonIdField forms a series in IoTDB.
     * @param model the KMX data model
     * @return a list of corresponding IoT data model
     */
    public static List<IoTModel> KMXToIoT(KMXModel model) {
        String deltaObj = getDeltaObj(model);
        List<IoTModel> ioTModels = new ArrayList<>();

        for(KMXField field : model.nonIdFields) {
            IoTModel ioTModel = new IoTModel();
            ioTModel.sampleTime = model.sampleTime;
            ioTModel.deltaObj = deltaObj;
            ioTModel.measurement = field.fieldId;
            ioTModel.value = field.fieldValue;
            ioTModels.add(ioTModel);
        }
        return ioTModels;
    }

    public static String getDeltaObj(KMXModel model) {
        model.IdFields.sort(null);
        StringBuilder builder = new StringBuilder("root");
        for(KMXField field : model.IdFields) {
            builder.append(".");
            builder.append(field.fieldId);
        }
        return builder.toString();
    }
}
