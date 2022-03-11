import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("10.10.80.31")
                .port(3306)
                .databaseList("dolphinscheduler")
                .tableList("dolphinscheduler.qrtz_scheduler_state")
                .username("root")
                .password("hadoopdb-hadooponeoneone@dc.com.")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.addSource(sourceFunction)
                .addSink(new MysqlSink());
        executionEnvironment.execute("MySqlBinlogSourceExample");
    }
}
