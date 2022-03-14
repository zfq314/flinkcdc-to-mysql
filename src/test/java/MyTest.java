import com.google.gson.Gson;
import utils.ConfigurationUtils;
import utils.ParseJsonDataUtils;

import java.util.HashMap;

public class MyTest {
    public static void main(String[] args) {
        String brokerList = ConfigurationUtils.getProperties("brokerList");
        System.out.println(brokerList);

        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        String cityName = "上海";
        String alisName = "魔都";
        hashMap.put("cityName", cityName);
        hashMap.put("alisName", alisName);
        Gson gson = new Gson();
        System.out.println(gson.toJson(hashMap));
        String jsonStr = "{\"id\":1001,\"name\":\"释迦摩尼\"}";
        //使用这种方式的话,y
        System.out.println(ParseJsonDataUtils.getJsonData(jsonStr));

    }
}
