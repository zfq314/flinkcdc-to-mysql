package flinkcdc2kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class MyKafkaUtil {
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){

        return new FlinkKafkaProducer<String>("10.10.80.31:9092", topic, new SimpleStringSchema());
    }

}
