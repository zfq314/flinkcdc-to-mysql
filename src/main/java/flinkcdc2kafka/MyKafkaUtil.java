package flinkcdc2kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utils.ConfigurationUtils;

public class MyKafkaUtil {
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        String brokerList = ConfigurationUtils.getProperties("brokerList");
        return new FlinkKafkaProducer<String>(brokerList, topic, new SimpleStringSchema());
    }

}
