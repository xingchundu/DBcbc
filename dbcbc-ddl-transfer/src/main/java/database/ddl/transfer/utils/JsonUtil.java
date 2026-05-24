package database.ddl.transfer.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 读取 classpath 下的类型映射 JSON（兼容打包为 jar 后的加载方式）
 */
public class JsonUtil {

    private static volatile Map<String, Map<String, String>> jsonMap = null;

    private JsonUtil() {
    }

    public static Map<String, Map<String, String>> readJsonData(String jsonPath) throws java.io.IOException {
        if (jsonMap == null) {
            synchronized (JsonUtil.class) {
                if (jsonMap == null) {
                    jsonMap = new ConcurrentHashMap<>();
                    ClassLoader cl = Thread.currentThread().getContextClassLoader();
                    if (cl == null) {
                        cl = JsonUtil.class.getClassLoader();
                    }
                    try (InputStream in = cl.getResourceAsStream(jsonPath)) {
                        if (in == null) {
                            throw new java.io.IOException("Classpath resource not found: " + jsonPath);
                        }
                        String jsonString = IOUtils.toString(in, StandardCharsets.UTF_8);
                        JSONObject jsonObject = JSONObject.parseObject(jsonString);
                        Set<String> keySet = jsonObject.keySet();
                        Iterator<String> iterator = keySet.iterator();
                        while (iterator.hasNext()) {
                            Map<String, String> mapingMap = new HashMap<>();
                            String convertType = iterator.next();
                            String mapping = jsonObject.getString(convertType);
                            JSONObject mappingJson = JSONObject.parseObject(mapping);
                            Set<String> orginalTypeSet = mappingJson.keySet();
                            for (String orginalType : orginalTypeSet) {
                                String targetType = mappingJson.getString(orginalType);
                                mapingMap.put(orginalType, targetType);
                                jsonMap.put(convertType, mapingMap);
                            }
                        }
                    }
                }
            }
        }

        return jsonMap;
    }
}
