package com.rainsoft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by CaoWeiDong on 2017-08-01.
 */
public class FieldConstants {
    //Oracle表字段
    public static final Map<String, String[]> ORACLE_TABLE_COLUMN_MAP = new HashMap<>();
    //BCP文件结构字段
    public static final Map<String, String[]> BCP_FILE_COLUMN_MAP = new HashMap<>();
    //需要检验的字段
    public static final Map<String, Set<String>> FILTER_COLUMN_MAP = new HashMap<>();
    //Solr不同的字段类型
    public static final Map<String, Set<String>> SOLR_FIELD_MAP = new HashMap<>();
    //数据类型Map
    public static final Map<String, String> DOC_TYPE_MAP = new HashMap<>();

    static {
        //获取BCP字段信息文件流
        InputStream in = FieldConstants.class.getClassLoader().getResourceAsStream("columns.json");
        try {
            //将BCP字段信息封装到Map
            String info = IOUtils.toString(in, "utf-8");
            JSONObject jsonObject = JSON.parseObject(info);

            //BCP文件结构字段
            JSONObject bcpJsonObject = jsonObject.getJSONObject("bcp-file");
            for (String key : bcpJsonObject.keySet()) {
                JSONArray jsonArray = bcpJsonObject.getJSONArray(key);
                String[] fields = jsonArray.toArray(new String[bcpJsonObject.size()]);
                BCP_FILE_COLUMN_MAP.put(key, fields);
            }

            //Oracle表字段
            JSONObject oracleJsonObject = jsonObject.getJSONObject("oracle-table");
            for (String key : oracleJsonObject.keySet()) {
                JSONArray jsonArray = jsonObject.getJSONArray(key);
                String[] fields = jsonArray.toArray(new String[jsonArray.size()]);
                ORACLE_TABLE_COLUMN_MAP.put(key, fields);
            }

            //需要检验的字段
            JSONObject checkJsonObject = jsonObject.getJSONObject("bcp-filter-field");
            for (String key : checkJsonObject.keySet()) {
                JSONArray jsonArray = jsonObject.getJSONArray(key);
                Set<String> fields = new HashSet<>(jsonArray.toJavaList(String.class));
                FILTER_COLUMN_MAP.put(key, fields);
            }

            //Solr字段类型
            JSONObject solrJsonObject = jsonObject.getJSONObject("solr-fields");
            for (String key : solrJsonObject.keySet()) {
                JSONArray jsonArray = jsonObject.getJSONArray(key);
                Set<String> fields = new HashSet<>(jsonArray.toJavaList(String.class));
                SOLR_FIELD_MAP.put(key, fields);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        DOC_TYPE_MAP.put("bbs", "论坛");
        DOC_TYPE_MAP.put("email", "邮件");
        DOC_TYPE_MAP.put("ftp", "文件");
        DOC_TYPE_MAP.put("http", "网页");
        DOC_TYPE_MAP.put("im_chat", "聊天");
        DOC_TYPE_MAP.put("real", "真实");
        DOC_TYPE_MAP.put("search", "搜索");
        DOC_TYPE_MAP.put("service", "场所");
        DOC_TYPE_MAP.put("vid", "虚拟");
        DOC_TYPE_MAP.put("weibo", "微博");
        DOC_TYPE_MAP.put("ending_trace", "ending_mac");

    }

}
