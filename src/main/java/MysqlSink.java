import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;

public class MysqlSink extends RichSinkFunction<String> {
    Connection connection;
    //预执行
    PreparedStatement iStmt, dStmt, uStmt;

    //返回Connection连接
    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = "jdbc:mysql://10.10.80.31:3306/dolphinscheduler?useSSL=false";
            //获取连接
            conn = DriverManager.getConnection(url, "root", "hadoopdb-hadooponeoneone@dc.com.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取连接
        connection = getConnection();
        String insertStatement = "insert into qrtz_scheduler_state_bak(SCHED_NAME,INSTANCE_NAME,LAST_CHECKIN_TIME,CHECKIN_INTERVAL) values(?,?,?,?)";
        String deleteStatement = "delete from qrtz_scheduler_state_bak where SCHED_NAME=?";
        String updateStatement = "update qrtz_scheduler_state_bak set LAST_CHECKIN_TIME=? where CHECKIN_INTERVAL=? ";

        iStmt = connection.prepareStatement(insertStatement);
        dStmt = connection.prepareStatement(deleteStatement);
        uStmt = connection.prepareStatement(updateStatement);
    }
    // 每条记录插入时调用一次

    @Override
    public void invoke(String value, Context context) throws Exception {
        //3> {"database":"dolphinscheduler","data":{"INSTANCE_NAME":"hadoop311644544982762","CHECKIN_INTERVAL":5000,"SCHED_NAME":"DolphinScheduler","LAST_CHECKIN_TIME":1646982617010},"type":"UPDATE","table":"qrtz_scheduler_state"}
        Gson gson = new Gson();

        HashMap<String, Object> hashMap = gson.fromJson(value, HashMap.class);
        String database = (String) hashMap.get("database");
        String table = (String) hashMap.get("table");
        String type = (String) hashMap.get("type");
        if ("dolphinscheduler".equals(database) && "qrtz_scheduler_state".equals(table)) {
            if ("INSERT".equals(type)) {
                System.out.println("insert => " + value);
                LinkedTreeMap<String, Object> data = (LinkedTreeMap<String, Object>) hashMap.get("data");
                String instance_name = (String) data.get("INSTANCE_NAME");
                Double checkin_interval = (Double) data.get("CHECKIN_INTERVAL");
                String sched_name = (String) data.get("SCHED_NAME");
                Double last_checkin_time = (Double) data.get("LAST_CHECKIN_TIME");
                iStmt.setString(1, sched_name);
                iStmt.setString(2, instance_name);
                iStmt.setDouble(3, last_checkin_time);
                iStmt.setDouble(4, checkin_interval);
                iStmt.executeUpdate();
            } else if ("DELETE".equals(type)) {
                System.out.println("delete => " + value);
                LinkedTreeMap<String, Object> data = (LinkedTreeMap<String, Object>) hashMap.get("data");
                String instance_name = (String) data.get("INSTANCE_NAME");
                dStmt.setString(1, instance_name);
                dStmt.executeUpdate();
            } else if ("UPDATE".equals(type)) {
                System.out.println("update => " + value);
                LinkedTreeMap<String, Object> data = (LinkedTreeMap<String, Object>) hashMap.get("data");
                Double checkin_interval = (Double) data.get("CHECKIN_INTERVAL");
                Double last_checkin_time = (Double) data.get("LAST_CHECKIN_TIME");
                uStmt.setDouble(1, last_checkin_time);
                uStmt.setDouble(2, checkin_interval);
                uStmt.executeUpdate();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (iStmt != null) {
            iStmt.close();
        }
        if (dStmt != null) {
            dStmt.close();
        }
        if (uStmt != null) {
            uStmt.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
