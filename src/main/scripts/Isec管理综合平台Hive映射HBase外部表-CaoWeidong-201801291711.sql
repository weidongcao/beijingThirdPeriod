--创建数据库
create database neirong;

-- BBS论坛表
CREATE EXTERNAL TABLE neirong.H_REG_CONTENT_BBS (
    ID                 string,
    SESSIONID          string,
    SERVICE_CODE       string,
    ROOM_ID            string,
    CERTIFICATE_TYPE   string,
    CERTIFICATE_CODE   string,
    USER_NAME          string,
    PROTOCOL_TYPE      string,
    ACCOUNT            string,
    PASSWD             string,
    URL                string,
    DOMAIN_NAME        string,
    REF_URL            string,
    REF_DOMAIN         string,
    POSTING_ID         string,
    TITLE              string,
    AUTHOR             string,
    SOURCE             string,
    ACTION_TYPE        string,
    SUMMARY            string,
    FILE_PATH          string,
    DEST_IP            string,
    DEST_PORT          string,
    SRC_IP             string,
    SRC_PORT           string,
    SRC_MAC            string,
    CAPTURE_TIME       string,
    CHECKIN_ID         string,
    DATA_SOURCE        string,
    MACHINE_ID         string,
    MANUFACTURER_CODE  string,
    ZIPNAME            string,
    BCPNAME            string,
    ROWNUMBER          string,
    IMPORT_TIME        string,
    SERVICE_CODE_IN    string,
    TERMINAL_LONGITUDE string,
    TERMINAL_LATITUDE  string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, INFO:SESSIONID ,INFO:SERVICE_CODE,INFO:ROOM_ID,INFO:CERTIFICATE_TYPE,INFO:CERTIFICATE_CODE ,INFO:USER_NAME,INFO:PROTOCOL_TYPE ,INFO:ACCOUNT,INFO:PASSWD,INFO:URL,INFO:DOMAIN_NAME,INFO:REF_URL,INFO:REF_DOMAIN,INFO:POSTING_ID,INFO:TITLE,INFO:AUTHOR,INFO:SOURCE,INFO:ACTION_TYPE,INFO:SUMMARY,INFO:FILE_PATH,INFO:DEST_IP,INFO:DEST_PORT,INFO:SRC_IP,INFO:SRC_PORT,INFO:SRC_MAC,INFO:CAPTURE_TIME,INFO:CHECKIN_ID,INFO:DATA_SOURCE,INFO:MACHINE_ID,INFO:MANUFACTURER_CODE,INFO:ZIPNAME,INFO:BCPNAME,INFO:ROWNUMBER,INFO:IMPORT_TIME,INFO:SERVICE_CODE_IN,INFO:TERMINAL_LONGITUDE,INFO:TERMINAL_LATITUDE")
TBLPROPERTIES("hbase.table.name" = "H_REG_CONTENT_BBS");

--邮件表
CREATE EXTERNAL TABLE neirong.H_REG_CONTENT_EMAIL (
    ID                 string,
    SESSIONID          string,
    SERVICE_CODE       string,
    ROOM_ID            string,
    CERTIFICATE_TYPE   string,
    CERTIFICATE_CODE   string,
    USER_NAME          string,
    PROTOCOL_TYPE      string,
    ACCOUNT            string,
    PASSWD             string,
    MAILID             string,
    SEND_TIME          string,
    MAIL_FROM          string,
    MAIL_TO            string,
    CC                 string,
    BCC                string,
    SUBJECT            string,
    SUMMARY            string,
    ATTACHMENT         string,
    FILE_PATH          string,
    ACTION_TYPE        string,
    DEST_IP            string,
    DEST_PORT          string,
    SRC_IP             string,
    SRC_PORT           string,
    SRC_MAC            string,
    CAPTURE_TIME       string,
    CHECKIN_ID         string,
    DATA_SOURCE        string,
    MACHINE_ID         string,
    MANUFACTURER_CODE  string,
    ZIPNAME            string,
    BCPNAME            string,
    ROWNUMBER          string,
    IMPORT_TIME        string,
    SERVICE_CODE_IN    string,
    TERMINAL_LONGITUDE string,
    TERMINAL_LATITUDE  string
 )
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:SESSIONID,INFO:SERVICE_CODE,INFO:ROOM_ID,INFO:CERTIFICATE_TYPE,INFO:CERTIFICATE_CODE,INFO:USER_NAME,INFO:PROTOCOL_TYPE,INFO:ACCOUNT,INFO:PASSWD,INFO:MAILID,INFO:SEND_TIME,INFO:MAIL_FROM,INFO:MAIL_TO,INFO:CC,INFO:BCC,INFO:SUBJECT,INFO:SUMMARY,INFO:ATTACHMENT,INFO:FILE_PATH,INFO:ACTION_TYPE,INFO:DEST_IP,INFO:DEST_PORT,INFO:SRC_IP,INFO:SRC_PORT,INFO:SRC_MAC,INFO:CAPTURE_TIME,INFO:CHECKIN_ID,INFO:DATA_SOURCE,INFO:MACHINE_ID,INFO:MANUFACTURER_CODE,INFO:ZIPNAME,INFO:BCPNAME,INFO:ROWNUMBER,INFO:IMPORT_TIME,INFO:SERVICE_CODE_IN,INFO:TERMINAL_LONGITUDE,INFO:TERMINAL_LATITUDE")
TBLPROPERTIES("hbase.table.name" = "H_REG_CONTENT_EMAIL");

--FTP文件表
CREATE EXTERNAL TABLE neirong.H_REG_CONTENT_FTP (
    ID                 string,
    SESSIONID          string,
    SERVICE_CODE       string,
    CERTIFICATE_TYPE   string,
    CERTIFICATE_CODE   string,
    USER_NAME          string,
    PROTOCOL_TYPE      string,
    ACCOUNT            string,
    PASSWD             string,
    FILE_NAME          string,
    FILE_PATH          string,
    ACTION_TYPE        string,
    IS_COMPLETED       string,
    DEST_IP            string,
    DEST_PORT          string,
    SRC_IP             string,
    SRC_PORT           string,
    SRC_MAC            string,
    CAPTURE_TIME       string,
    ROOM_ID            string,
    CHECKIN_ID         string,
    MACHINE_ID         string,
    DATA_SOURCE        string,
    FILE_SIZE          string,
    FILE_URL           string,
    MANUFACTURER_CODE  string,
    ZIPNAME            string,
    BCPNAME            string,
    ROWNUMBER          string,
    IMPORT_TIME        string,
    SERVICE_CODE_IN    string,
    TERMINAL_LONGITUDE string,
    TERMINAL_LATITUDE  string
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:SESSIONID ,INFO:SERVICE_CODE ,INFO:CERTIFICATE_TYPE,INFO:CERTIFICATE_CODE, INFO:USER_NAME ,INFO:PROTOCOL_TYPE,INFO:ACCOUNT,INFO:PASSWD ,INFO:FILE_NAME,INFO:FILE_PATH,INFO:ACTION_TYPE,INFO:IS_COMPLETED,INFO:DEST_IP,INFO:DEST_PORT,INFO:SRC_IP,INFO:SRC_PORT,INFO:SRC_MAC,INFO:CAPTURE_TIME,INFO:ROOM_ID,INFO:CHECKIN_ID,INFO:MACHINE_ID,INFO:DATA_SOURCE,INFO:FILE_SIZE,INFO:FILE_URL,INFO:MANUFACTURER_CODE,INFO:ZIPNAME,INFO:BCPNAME,INFO:ROWNUMBER,INFO:IMPORT_TIME,INFO:SERVICE_CODE_IN,INFO:TERMINAL_LONGITUDE,INFO:TERMINAL_LATITUDE")
TBLPROPERTIES("hbase.table.name" = "H_REG_CONTENT_FTP");

--HTTP网页表
CREATE EXTERNAL TABLE neirong.H_REG_CONTENT_HTTP (
    ID                 string,
    SESSIONID          string,
    SERVICE_CODE       string,
    CERTIFICATE_TYPE   string,
    CERTIFICATE_CODE   string,
    USER_NAME          string,
    PROTOCOL_TYPE      string,
    URL                string,
    DOMAIN_NAME        string,
    REF_URL            string,
    REF_DOMAIN         string,
    ACTION_TYPE        string,
    SUBJECT            string,
    SUMMARY            string,
    COOKIE_PATH        string,
    UPLOAD_FILE        string,
    DOWNLOAD_FILE      string,
    DEST_IP            string,
    DEST_PORT          string,
    SRC_IP             string,
    SRC_PORT           string,
    SRC_MAC            string,
    CAPTURE_TIME       string,
    ROOM_ID            string,
    CHECKIN_ID         string,
    MACHINE_ID         string,
    DATA_SOURCE        string,
    MANUFACTURER_CODE  string,
    ZIPNAME            string,
    BCPNAME            string,
    ROWNUMBER          string,
    IMPORT_TIME        string,
    SERVICE_CODE_IN    string,
    TERMINAL_LONGITUDE string,
    TERMINAL_LATITUDE  string
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:SESSIONID,INFO:SERVICE_CODE,INFO:CERTIFICATE_TYPE,INFO:CERTIFICATE_CODE,INFO:USER_NAME,INFO:PROTOCOL_TYPE,INFO:URL,INFO:DOMAIN_NAME,INFO:REF_URL,INFO:REF_DOMAIN,INFO:ACTION_TYPE,INFO:SUBJECT,INFO:SUMMARY,INFO:COOKIE_PATH,INFO:UPLOAD_FILE,INFO:DOWNLOAD_FILE,INFO:DEST_IP,INFO:DEST_PORT,INFO:SRC_IP,INFO:SRC_PORT,INFO:SRC_MAC,INFO:CAPTURE_TIME,INFO:ROOM_ID,INFO:CHECKIN_ID,INFO:MACHINE_ID,INFO:DATA_SOURCE,INFO:MANUFACTURER_CODE ,INFO:ZIPNAME,INFO:BCPNAME,INFO:ROWNUMBER,INFO:IMPORT_TIME,INFO:SERVICE_CODE_IN,INFO:TERMINAL_LONGITUDE,INFO:TERMINAL_LATITUDE")
TBLPROPERTIES("hbase.table.name" = "H_REG_CONTENT_HTTP");

--聊天表
CREATE EXTERNAL TABLE neirong.H_REG_CONTENT_IM_CHAT (
    ID                 string,
    SESSIONID          string,
    SERVICE_CODE       string,
    ROOM_ID            string,
    CERTIFICATE_TYPE   string,
    CERTIFICATE_CODE   string,
    USER_NAME          string,
    PROTOCOL_TYPE      string,
    ACCOUNT            string,
    ACOUNT_NAME        string,
    FRIEND_ACCOUNT     string,
    FRIEND_NAME        string,
    CHAT_TYPE          string,
    SENDER_ACCOUNT     string,
    SENDER_NAME        string,
    CHAT_TIME          string,
    DEST_IP            string,
    DEST_PORT          string,
    SRC_IP             string,
    SRC_PORT           string,
    SRC_MAC            string,
    CAPTURE_TIME       string,
    MSG                string,
    CHECKIN_ID         string,
    DATA_SOURCE        string,
    MACHINE_ID         string,
    MANUFACTURER_CODE  string,
    ZIPNAME            string,
    BCPNAME            string,
    ROWNUMBER          string,
    IMPORT_TIME        string,
    SERVICE_CODE_IN    string,
    TERMINAL_LONGITUDE string,
    TERMINAL_LATITUDE  string
 )
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:SESSIONID,INFO:SERVICE_CODE ,INFO:ROOM_ID,INFO:CERTIFICATE_TYPE ,INFO:CERTIFICATE_CODE ,INFO:USER_NAME,INFO:PROTOCOL_TYPE,INFO:ACCOUNT,INFO:ACOUNT_NAME,INFO:FRIEND_ACCOUNT,INFO:FRIEND_NAME,INFO:CHAT_TYPE,INFO:SENDER_ACCOUNT,INFO:SENDER_NAME,INFO:CHAT_TIME,INFO:DEST_IP,INFO:DEST_PORT,INFO:SRC_IP,INFO:SRC_PORT,INFO:SRC_MAC,INFO:CAPTURE_TIME,INFO:MSG ,INFO:CHECKIN_ID ,INFO:DATA_SOURCE ,INFO:MACHINE_ID ,INFO:MANUFACTURER_CODE,INFO:ZIPNAME,INFO:BCPNAME,INFO:ROWNUMBER,INFO:IMPORT_TIME,INFO:SERVICE_CODE_IN,INFO:TERMINAL_LONGITUDE,INFO:TERMINAL_LATITUDE")
TBLPROPERTIES("hbase.table.name" = "H_REG_CONTENT_IM_CHAT");

--搜索
CREATE EXTERNAL TABLE neirong.H_REG_CONTENT_SEARCH (
    ID                 string,
    SESSIONID          string,
    SERVICE_CODE       string,
    ROOM_ID            string,
    CERTIFICATE_TYPE   string,
    CERTIFICATE_CODE   string,
    USER_NAME          string,
    PROTOCOL_TYPE      string,
    URL                string,
    DOMAIN_NAME        string,
    REF_URL            string,
    REF_DOMAIN         string,
    KEYWORD            string,
    KEYWORD_CODE       string,
    DEST_IP            string,
    DEST_PORT          string,
    SRC_IP             string,
    SRC_PORT           string,
    SRC_MAC            string,
    CAPTURE_TIME       string,
    CHECKIN_ID         string,
    MACHINE_ID         string,
    DATA_SOURCE        string,
    MANUFACTURER_CODE  string,
    ZIPNAME            string,
    BCPNAME            string,
    ROWNUMBER          string,
    IMPORT_TIME        string,
    SERVICE_CODE_IN    string,
    TERMINAL_LONGITUDE string,
    TERMINAL_LATITUDE  string
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:SESSIONID,INFO:SERVICE_CODE,INFO:ROOM_ID,INFO:CERTIFICATE_TYPE,INFO:CERTIFICATE_CODE,INFO:USER_NAME ,INFO:PROTOCOL_TYPE,INFO:URL,INFO:DOMAIN_NAME,INFO:REF_URL,INFO:REF_DOMAIN,INFO:KEYWORD,INFO:KEYWORD_CODE,INFO:DEST_IP,INFO:DEST_PORT,INFO:SRC_IP,INFO:SRC_PORT,INFO:SRC_MAC,INFO:CAPTURE_TIME,INFO:CHECKIN_ID,INFO:MACHINE_ID,INFO:DATA_SOURCE,INFO:MANUFACTURER_CODE ,INFO:ZIPNAME,INFO:BCPNAME,INFO:ROWNUMBER,INFO:IMPORT_TIME,INFO:SERVICE_CODE_IN,INFO:TERMINAL_LONGITUDE,INFO:TERMINAL_LATITUDE")
TBLPROPERTIES("hbase.table.name" = "H_REG_CONTENT_SEARCH");

--真实
CREATE EXTERNAL TABLE neirong.H_REG_REALID_INFO (
    ID                 string,
    CERTIFICATE_TYPE    string,
    CERTIFICATE_CODE    string,
    USER_NAME           string,
    SEX                 string,
    BIRTHDAY            string,
    PEOPLE              string,
    COUNTRY             string,
    ORG_NAME            string,
    REMARK              string,
    LAST_LOGINTIME      string,
    LAST_SERVICE_CODE   string,
    USE_NUMS            string,
    FIRST_LOGINTIME     string,
    FIRST_SERVICE_CODE  string,
    FIRST_ROOM_ID       string,
    LAST_ROOM_ID        string,
    QQ_NUMS             string,
    USER_ADDRESS        string,
    USER_PHONE          string,
    ID_EXPIRETIME       string
  )
 ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:CERTIFICATE_TYPE,INFO:CERTIFICATE_CODE,INFO:USER_NAME,INFO:SEX,INFO:BIRTHDAY,INFO:PEOPLE,INFO:COUNTRY,INFO:ORG_NAME,INFO:REMARK,INFO:LAST_LOGINTIME,INFO:LAST_SERVICE_CODE,INFO:USE_NUMS,INFO:FIRST_LOGINTIME,INFO:FIRST_SERVICE_CODE,INFO:FIRST_ROOM_ID,INFO:LAST_ROOM_ID,INFO:QQ_NUMS,INFO:USER_ADDRESS,INFO:USER_PHONE,INFO:ID_EXPIRETIME")
TBLPROPERTIES("hbase.table.name" = "H_REG_REALID_INFO");

--虚拟
CREATE EXTERNAL TABLE neirong.H_REG_VID_INFO (
    ID                 string,
    PROTOCOL_TYPE      string,
    ACCOUNT            string,
    NICK_NAME          string,
    PASSWD             string,
    USE_NUMS           string,
    LAST_LOGINTIME     string,
    LAST_SERVICE_CODE  string,
    FIRST_LOGINTIME    string,
    FIRST_SERVICE_CODE string,
    FIRST_ROOM_ID      string,
    LAST_ROOM_ID       string,
    REALID_NUMS        string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:PROTOCOL_TYPE ,INFO:ACCOUNT ,INFO:NICK_NAME,INFO:PASSWD,INFO:USE_NUMS,INFO:LAST_LOGINTIME,INFO:LAST_SERVICE_CODE,INFO:FIRST_LOGINTIME,INFO:FIRST_SERVICE_CODE,INFO:FIRST_ROOM_ID,INFO:LAST_ROOM_ID,INFO:REALID_NUMS")
TBLPROPERTIES("hbase.table.name" = "H_REG_VID_INFO");

--场所
CREATE EXTERNAL TABLE neirong.H_SERVICE_INFO (
    GROUP_ID                       string,
    SERVICE_TYPE                   string,
    SERVICE_CODE                   string,
    SERVICE_NAME                   string,
    ADDRESS                        string,
    POSTAL_CODE                    string,
    PRINCIPAL                      string,
    PRINCIPAL_TEL                  string,
    INFOR_MAN                      string,
    INFOR_MAN_TEL                  string,
    INFOR_MAN_EMAIL                string,
    ISP                            string,
    STATUS                         string,
    ENDING_NUMS                    string,
    SERVER_NUMS                    string,
    SERVICE_IP                     string,
    NET_TYPE                       string,
    PRACTITIONER_COUNT             string,
    NET_MONITOR_DEPARTMENT         string,
    NET_MONITOR_MAN                string,
    NET_MONITOR_MAN_TEL            string,
    REMARK                         string,
    DATA_SOURCE                    string,
    MACHINE_ID                     string,
    SERVICE_NAME_PIN_YIN           string,
    ONLINE_STATUS                  string,
    UPDATE_TIME                    string,
    PROBE_VERSION                  string,
    TEMPLET_VERSION                string,
    LONGITUDE_LATITUDE             string,
    SPACE_SIZE                     string,
    LATITUDE                       string,
    IP_ADDRESS                     string,
    IS_OUTLINE_ALERT               string,
    ELEVATION                      string,
    SERVICE_IMG                    string,
    AGEN_LAVE_TIME                 string,
    REAL_ENDING_NUMS               string,
    CAUSE                          string,
    IS_ALLOW_INSERT                string,
    BUSINESS_NATURE                string,
    LAW_PRINCIPAL_CERTIFICATE_TYPE string,
    LAW_PRINCIPAL_CERTIFICATE_ID   string,
    START_TIME                     string,
    END_TIME                       string,
    IS_READ                        string,
    ZIPNAME                        string,
    BCPNAME                        string,
    ROWNUMBER                      string,
    INSTALL_TIME                   string,
    POLIC_STATION                  string,
    MANUFACTURER_CODE              string,
    AREA_CODE                      string,
    IS_VIRTUAL                     string,
    IF_CHECK                       string,
    SERVICE_CODE_IN                string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:SERVICE_TYPE,INFO:SERVICE_CODE,INFO:SERVICE_NAME,INFO:ADDRESS,INFO:POSTAL_CODE,INFO:PRINCIPAL,INFO:PRINCIPAL_TEL,INFO:INFOR_MAN,INFO:INFOR_MAN_TEL,INFO:INFOR_MAN_EMAIL,INFO:ISP,INFO:STATUS,INFO:ENDING_NUMS,INFO:SERVER_NUMS,INFO:SERVICE_IP,INFO:NET_TYPE,INFO:PRACTITIONER_COUNT,INFO:NET_MONITOR_DEPARTMENT,INFO:NET_MONITOR_MAN,INFO:NET_MONITOR_MAN_TEL,INFO:REMARK,INFO:DATA_SOURCE,INFO:MACHINE_ID,INFO:SERVICE_NAME_PIN_YIN,INFO:ONLINE_STATUS,INFO:UPDATE_TIME,INFO:PROBE_VERSION,INFO:TEMPLET_VERSION,INFO:LONGITUDE_LATITUDE,INFO:SPACE_SIZE,INFO:LATITUDE,INFO:IP_ADDRESS,INFO:IS_OUTLINE_ALERT,INFO:ELEVATION,INFO:SERVICE_IMG,INFO:AGEN_LAVE_TIME,INFO:REAL_ENDING_NUMS,INFO:CAUSE,INFO:IS_ALLOW_INSERT,INFO:BUSINESS_NATURE,INFO:LAW_PRINCIPAL_CERTIFICATE_TYPE,INFO:LAW_PRINCIPAL_CERTIFICATE_ID,INFO:START_TIME,INFO:END_TIME,INFO:IS_READ,INFO:ZIPNAME,INFO:BCPNAME,INFO:ROWNUMBER,INFO:INSTALL_TIME,INFO:POLIC_STATION,INFO:MANUFACTURER_CODE,INFO:AREA_CODE,INFO:IS_VIRTUAL,INFO:IF_CHECK,INFO:SERVICE_CODE_IN")
TBLPROPERTIES("hbase.table.name" = "H_SERVICE_INFO");

--微博
CREATE EXTERNAL TABLE neirong.H_REG_CONTENT_WEIBO (
    ID                 string,
    SESSIONID          string,
    SERVICE_CODE       string,
    ROOM_ID            string,
    CERTIFICATE_TYPE   string,
    CERTIFICATE_CODE   string,
    USER_NAME          string,
    PROTOCOL_TYPE      string,
    ACCOUNT            string,
    PASSWD             string,
    URL                string,
    DOMAIN_NAME        string,
    REF_URL            string,
    REF_DOMAIN         string,
    POSTING_ID         string,
    TITLE              string,
    AUTHOR             string,
    SOURCE             string,
    ACTION_TYPE        string,
    SUMMARY            string,
    FILE_PATH          string,
    DEST_IP            string,
    DEST_PORT          string,
    SRC_IP             string,
    SRC_PORT           string,
    SRC_MAC            string,
    CAPTURE_TIME       string,
    CHECKIN_ID         string,
    MACHINE_ID         string,
    DATA_SOURCE        string,
    MANUFACTURER_CODE  string,
    ZIPNAME            string,
    BCPNAME            string,
    ROWNUMBER          string,
    IMPORT_TIME        string,
    SERVICE_CODE_OUT   string,
    TERMINAL_LONGITUDE string,
    TERMINAL_LATITUDE  string,
    CERTIFICATE_NAME   string,
    COORDINATE_TYPE    string,
    LONGITUDE_BAIDU    string,
    LATITUDE_BAIDU     string,
    LONGITUDE_HARDWARE string,
    LATITUDE_HARDWARE  string,
    IS_UPDATED         string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:SESSIONID ,INFO:SERVICE_CODE,INFO:ROOM_ID,INFO:CERTIFICATE_TYPE,INFO:CERTIFICATE_CODE,INFO:USER_NAME,INFO:PROTOCOL_TYPE,INFO:ACCOUNT,INFO:PASSWD,INFO:URL,INFO:DOMAIN_NAME,INFO:REF_URL,INFO:REF_DOMAIN,INFO:POSTING_ID,INFO:TITLE,INFO:AUTHOR,INFO:SOURCE,INFO:ACTION_TYPE,INFO:SUMMARY,INFO:FILE_PATH,INFO:DEST_IP,INFO:DEST_PORT,INFO:SRC_IP,INFO:SRC_PORT,INFO:SRC_MAC,INFO:CAPTURE_TIME,INFO:CHECKIN_ID,INFO:MACHINE_ID,INFO:DATA_SOURCE ,INFO:MANUFACTURER_CODE ,INFO:ZIPNAME ,INFO:BCPNAME ,INFO:ROWNUMBER,INFO:IMPORT_TIME ,INFO:SERVICE_CODE_OUT,INFO:TERMINAL_LONGITUDE,INFO:TERMINAL_LATITUDE,INFO:CERTIFICATE_NAME,INFO:COORDINATE_TYPE, INFO:LONGITUDE_BAIDU, INFO:LATITUDE_BAIDU, INFO:LONGITUDE_HARDWARE, INFO:LATITUDE_HARDWARE, INFO:IS_UPDATED")
TBLPROPERTIES("hbase.table.name" = "H_REG_CONTENT_WEIBO");

CREATE EXTERNAL TABLE neirong.H_SYS_DICT (
    ID         string,
    DICT_CODE  string,
    DICT_NAME  string,
    REMARK     string,
    PARENT_ID  string,
    STATUS     string,
    RESERVE_STATUS  string
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INFO:DICT_CODE,INFO:DICT_NAME,INFO:REMARK,INFO:PARENT_ID,INFO:STATUS,INFO:RESERVE_STATUS")
TBLPROPERTIES("hbase.table.name" = "H_SYS_DICT");

--字段身份证与IP做转换
sqoop import  --connect   jdbc:oracle:thin:@172.16.100.18:1521/orcl --username yunan --password yunan --m 1   --query  " select SESSIONID,SERVICE_CODE,ROOM_ID,CERTIFICATE_TYPE,CERTIFICATE_CODE,b.DICT_NAME as CERTIFICATE_NAME,USER_NAME,PROTOCOL_TYPE,ACCOUNT,PASSWD,URL,DOMAIN_NAME,REF_URL,REF_DOMAIN,POSTING_ID,TITLE,AUTHOR,SOURCE,ACTION_TYPE,SUMMARY,FILE_PATH,fun_int2ip(dest_ip) AS dest_ip,DEST_PORT,fun_int2ip(SRC_IP) src_ip,SRC_PORT,SRC_MAC, CAPTURE_TIME,CHECKIN_ID, DATA_SOURCE,MACHINE_ID,IMPORT_TIME,a.ID from REG_CONTENT_WEIBO a LEFT JOIN SYS_DICT b on b.PARENT_ID = '100' and b.DICT_CODE = a.CERTIFICATE_TYPE where \$CONDITIONS "   --hbase-create-table --hbase-table H_REG_CONTENT_WEIBO --hbase-row-key ID --column-family CONTENT_WEIBO
