package utils;


import com.alibaba.fastjson.JSONObject;

public class ParseJsonDataUtils {
    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
