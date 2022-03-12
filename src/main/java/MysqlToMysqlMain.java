import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlToMysqlMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        executionEnvironment.enableCheckpointing(3000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(1500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        executionEnvironment.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment, settings);
        streamTableEnvironment.getConfig().setSqlDialect(SqlDialect.DEFAULT);


        //数据源表
        String sourceDDL =
                "CREATE TABLE data_log(\n" +
                        "SCHED_NAME STRING,\n" +
                        "INSTANCE_NAME STRING,\n" +
                        "LAST_CHECKIN_TIME DOUBLE,\n" +
                        "CHECKIN_INTERVAL DOUBLE,\n" +
                        "primary key(SCHED_NAME,INSTANCE_NAME) not enforced\n" +
                        ") WITH(\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = '10.10.80.31',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'hadoopdb-hadooponeoneone@dc.com.',\n" +
                        " 'database-name' = 'dolphinscheduler',\n" +
                        " 'table-name' = 'qrtz_scheduler_state',\n" +
                        " 'scan.startup.mode' = 'latest-offset'\n" +
                        ")";
        // 输出目标表
        String sinkDDL =
                "CREATE TABLE qrtz_scheduler_state_bak(\n" +
                        "SCHED_NAME STRING,\n" +
                        "INSTANCE_NAME STRING,\n" +
                        "LAST_CHECKIN_TIME DOUBLE,\n" +
                        "CHECKIN_INTERVAL DOUBLE,\n" +
                        "primary key(SCHED_NAME,INSTANCE_NAME) not enforced\n" +
                        ") WITH(\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        " 'url' = 'jdbc:mysql://10.10.80.31:3306/dolphinscheduler?serverTimezone=UTC&useSSL=false',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'hadoopdb-hadooponeoneone@dc.com.',\n" +
                        " 'table-name' = 'qrtz_scheduler_state_bak'\n" +
                        ")";
        String transformDmlSQL = "insert into qrtz_scheduler_state_bak select * from data_log";
        streamTableEnvironment.executeSql(sourceDDL);
        streamTableEnvironment.executeSql(sinkDDL);
        streamTableEnvironment.executeSql(transformDmlSQL);

       // executionEnvironment.execute("MysqlToMysqlMain");

    }
}
