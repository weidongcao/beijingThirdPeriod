package com.rainsoft.utils;

import com.rainsoft.conf.ConfigurationManager;

import java.io.File;

/**
 * 命名规则工具类
 * Created by Administrator on 2017-10-16.
 */
public class NamingRuleUtils {
    /**
     * Oracle内容表的命名规则
     * reg_content_${任务类型}
     * @param task 任务类型
     * @return String
     */
    public static String getOracleContentTableName(String task) {
        if ("service".equalsIgnoreCase(task)) {
            return "service_info";
        } else if ("real".equalsIgnoreCase(task)) {
            return "reg_realid_info";
        } else if ("vid".equalsIgnoreCase(task)) {
            return "reg_vid_info";
        } else {
            return "reg_content_" + task.toLowerCase();
        }
    }

    /**
     * HBase表的命名规则
     * H_${Oracle表名大写}
     * @param task 任务类型
     * @return HBase表名
     */
    public static String getHBaseTableName(String task) {
        return "H_" + getOracleContentTableName(task).toUpperCase();
    }

    public static String getTmpHBaseTableName(String task) {
        return getHBaseTableName(task) + "_TMP";
    }

    /**
     * 内容表HBase列簇的命令规则
     * CONTENT_${任务类型}
     * @param task 任务类型
     * @return HBase表列簇
     */
    public static String getHBaseContentTableCF(String task) {
        if ("service".equalsIgnoreCase(task)) {
            return "SERVICE_INFO";
        } else if ("real".equalsIgnoreCase(task)) {
            return "REALID_INFO";
        } else if ("vid".equalsIgnoreCase(task)) {
            return "VID_INFO";
        } else {
            return "CONTENT_" + task.toUpperCase();
        }
    }

    /**
     * 从oracle实时导出数据,导出记录的key
     * @param task 任务类型
     * @return 任务实时导入的RowKey
     */
    public static String getRealTimeOracleRecordKey(String task) {
        return getOracleContentTableName(task).toLowerCase() + "-last_export_time";
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
        return ConfigurationManager.getProperty("bcp.file.path") + File.separator + task;
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
