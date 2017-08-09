package com.rainsoft;

import java.util.HashMap;
import java.util.Map;
import java.sql.Types;

/**
 * Created by CaoWeiDong on 2017-08-01.
 */
public class FieldConstants {
    //Oracle表及对应的字段
    public static final Map<String, String[]> BCP_FIELD_MAP = new HashMap<>();

    //Oracle字段对应的类型
    public static final Map<String, Integer> ORACLE_Field_TO_JAVA_TYPE = new HashMap<>();

    public static final Map<String, String> DOC_TYPE_MAP = new HashMap<>();

    static {
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

        BCP_FIELD_MAP.put("ftp", new String[]{
                "service_code",
                "sessionid",
                "certificate_type",
                "certificate_code",
                "user_name",
                "protocol_type",
                "account",
                "passwd",
                "file_name",
                "file_path",
                "action_type",
                "is_completed",
                "dest_ip",
                "dest_port",
                "src_ip",
                "src_port",
                "src_mac",
                "capture_time",
                "room_id",
                "checkin_id",
                "machine_id",
                "data_source",
                "file_size",
                "file_url",
                "manufacturer_code",
                "zipname",
                "bcpname",
                "rownumber",
                "service_code_out",
                "terminal_longitude",
                "terminal_latitude"
        });
        BCP_FIELD_MAP.put("im_chat", new String[]{
                "service_code",
                "sessionid",
                "certificate_type",
                "certificate_code",
                "user_name",
                "protocol_type",
                "account",
                "acount_name",
                "friend_account",
                "friend_name",
                "sender_account",
                "sender_name",
                "chat_type",
                "msg",
                "chat_time",
                "dest_ip",
                "dest_port",
                "src_ip",
                "src_port",
                "src_mac",
                "capture_time",
                "room_id",
                "checkin_id",
                "machine_id",
                "data_source",
                "manufacturer_code",
                "zipname",
                "bcpname",
                "rownumber",
                "service_code_out",
                "terminal_longitude",
                "terminal_latitude"
        });
        BCP_FIELD_MAP.put("http", new String[]{
                "service_code",
                "sessionid",
                "certificate_type",
                "certificate_code",
                "user_name",
                "protocol_type",
                "url",
                "domain_name",
                "ref_url",
                "ref_domain",
                "action_type",
                "subject",
                "summary",
                "cookie_path",
                "zip_path",
                "upload_file",
                "download_file",
                "dest_ip",
                "dest_port",
                "src_ip",
                "src_port",
                "src_mac",
                "capture_time",
                "room_id",
                "checkin_id",
                "machine_id",
                "data_source",
                "manufacturer_code",
                "zipname",
                "bcpname",
                "rownumber",
                "service_code_out",
                "terminal_longitude",
                "terminal_latitude"
        });
    }
}
