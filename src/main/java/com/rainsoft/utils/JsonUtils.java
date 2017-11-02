package com.rainsoft.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by CaoWeiDong on 2017-07-29.
 */
public class JsonUtils {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);
    /**
     * 根据json配置文件及key获取Value类型为JSONArray的值
     * @param path 配置文件路径,此文件一定要在根目录下
     * @param key key
     * @return value
     */
    public static JSONArray getJsonValueByFile(String path, String key) {
        InputStream in = JsonUtils.class.getClassLoader().getResourceAsStream(path);
        String oracleTableFieldsJson = null;
        try {
            oracleTableFieldsJson = IOUtils.toString(in, "utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONObject jsonObject = JSONObject.fromObject(oracleTableFieldsJson);
        return jsonObject.optJSONArray(key);
    }
}
