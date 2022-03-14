package utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 读取配置文件工具类
 */
public class ConfigurationUtils {
    private static Properties properties = new Properties();

    //读取Resource目录下文件
    static {
        InputStream resourceAsStream = ConfigurationUtils.class.getClassLoader().getResourceAsStream("custom.properties");
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperties(String key) {

        return properties.getProperty(key);
    }

    //获取boolean类型的配置项
    public static boolean getBoolean(String key) {
        String myKey = properties.getProperty(key);
        try {
            return Boolean.valueOf(myKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
