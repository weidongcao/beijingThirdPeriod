package com.rainsoft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by CaoWeiDong on 2017-08-01.
 */
public class FieldConstants {
    //字段
    public static final Map<String, String[]> COLUMN_MAP = new HashMap<>();

    //数据类型Map
    public static final Map<String, String> DOC_TYPE_MAP = new HashMap<>();

    static {
        //获取BCP字段信息文件流
        InputStream in = FieldConstants.class.getClassLoader().getResourceAsStream("columns.json");
        try {
            //将BCP字段信息封装到Map
            String tableInfo = IOUtils.toString(in, "utf-8");
            JSONObject jsonObject = JSON.parseObject(tableInfo);
            for (String tableName : jsonObject.keySet()) {
                JSONArray jsonArray = jsonObject.getJSONArray(tableName);
                String[] fields = jsonArray.toArray(new String[jsonArray.size()]);
                COLUMN_MAP.put(tableName, fields);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        DOC_TYPE_MAP.put("ftp", "文件");
        DOC_TYPE_MAP.put("im_chat", "聊天");
        DOC_TYPE_MAP.put("http", "网页");
        DOC_TYPE_MAP.put("bbs", "论坛");
        DOC_TYPE_MAP.put("email", "邮件");
        DOC_TYPE_MAP.put("weibo", "微博");
        DOC_TYPE_MAP.put("search", "搜索");
        DOC_TYPE_MAP.put("real", "真实");
        DOC_TYPE_MAP.put("vid", "虚拟");
        DOC_TYPE_MAP.put("service", "场所");

    }

}
