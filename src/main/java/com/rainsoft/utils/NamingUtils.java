package com.rainsoft.utils;

import com.rainsoft.conf.ConfigurationManager;

import java.io.File;

/**
 * 命名规则工具类
 * Created by Administrator on 2017-10-16.
 */
public class NamingUtils {
    private static String bcpWorkRootDir = ConfigurationManager.getProperty("bcp.file.path");
    /**
     * Oracle内容表的命名规则
     * reg_content_${任务类型}
     * @param task 任务类型
     * @return String
     */
    public static String getTableName(String task) {
        String tableName;
        switch (task) {
            case "service":
                tableName =  "service_info";
                break;
            case "real":
                tableName =  "reg_realid_info";
                break;
            case "vid":
                tableName =  "reg_vid_info";
                break;
            case "imsi":
                tableName =  "scan_imsi_info";
                break;
            case "imei":
                tableName =  "imei_info";
                break;
            default:
                tableName =  "reg_content_" + task.toLowerCase();
        }
        return tableName;
    }

    /**
     * HBase表的命名规则
     * H_${Oracle表名大写}
     * @param task 任务类型
     * @return HBase表名
     */
    public static String getHBaseTableName(String task) {
        return "H_" + getTableName(task).toUpperCase();
    }

    public static String getTmpHBaseTableName(String task) {
        return getHBaseTableName(task) + "_TMP";
    }

    /**
     * 内容表HBase列簇的命令规则
     * CONTENT_${任务类型}
     * @return HBase表列簇
     */
    public static String getHBaseContentTableCF() {
        return "INFO";
    }

    /**
     * 从oracle实时导出数据,导出记录的key
     * @param task 任务类型
     * @return 任务实时导入的RowKey
     */
    public static String getOracleRecordKey(String task) {
        return getTableName(task).toLowerCase() + "-last_export_time";
    }

    /**
     * Bcp类型的数据task的Key
     * 就像Oracle对应的reg_content_ftp一样
     * @param task 任务类型
     * @return Bcp任务的key
     */
    public static String getBcpTaskKey(String task) {
        return "bcp_" + task.toLowerCase();
    }

    /**
     * 获取HDFS文件系统上HFile文件的临时存储目录
     * @param task 任务类型
     * @return HFile临时路径
     */
    public static String getHFileTaskDir(String task) {
        return "/tmp/hbase/hfile/" + task + "/";
    }

    /**
     * 获取Bcp文件的工作目录
     * @param task 任务类型
     * @return Bcp本地工作目录
     */
    public static String getBcpWorkDir(String task) {
        return  bcpWorkRootDir + File.separator + task;
    }

    /**
     * Bcp需要过滤的关键字段
     * @param task 任务类型
     * @return Bcp过滤数组的key
     */
    public static String getBcpFilterKey(String task) {
        return "filter-" + getBcpTaskKey(task);
    }

}
