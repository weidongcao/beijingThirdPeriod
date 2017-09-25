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

    //Oracle字段对应的类型
    public static final Map<String, Integer> ORACLE_Field_TO_JAVA_TYPE = new HashMap<>();

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

        ORACLE_Field_TO_JAVA_TYPE.put("account", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("acount_name", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("action_type", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("bcpname", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("capture_time", Types.DATE);
        ORACLE_Field_TO_JAVA_TYPE.put("certificate_code", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("certificate_type", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("chat_time", Types.DATE);
        ORACLE_Field_TO_JAVA_TYPE.put("chat_type", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("checkin_id", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("cookie_path", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("data_source", Types.CHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("dest_ip", Types.INTEGER);
        ORACLE_Field_TO_JAVA_TYPE.put("dest_port", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("domain_name", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("download_file", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("file_name", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("file_path", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("file_size", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("file_url", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("friend_account", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("friend_name", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("is_completed", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("id", Types.BIGINT);
        ORACLE_Field_TO_JAVA_TYPE.put("machine_id", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("manufacturer_code", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("msg", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("passwd", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("protocol_type", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("ref_domain", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("ref_url", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("room_id", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("rownumber", Types.BIGINT);
        ORACLE_Field_TO_JAVA_TYPE.put("sender_account", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("sender_name", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("service_code", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("service_code_out", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("sessionid", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("src_ip", Types.BIGINT);
        ORACLE_Field_TO_JAVA_TYPE.put("src_mac", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("src_port", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("subject", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("summary", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("terminal_latitude", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("terminal_longitude", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("upload_file", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("url", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("user_name", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("zip_path", Types.VARCHAR);
        ORACLE_Field_TO_JAVA_TYPE.put("zipname", Types.VARCHAR);


    }

}
