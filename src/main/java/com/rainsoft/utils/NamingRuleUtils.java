package com.rainsoft.utils;

/**
 * 命名规则工具类
 * Created by Administrator on 2017-10-16.
 */
public class NamingRuleUtils {
    /**
     * Oracle内容表的命名规则
     * reg_content_${任务类型}
     * @param task 任务类型
     * @return
     */
    public static String getOracleContentTableName(String task) {
        return "reg_content_" + task.toLowerCase();
    }

    /**
     * HBase表的命名规则
     * H_${Oracle表名大写}
     * @param task
     * @return
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
     * @param task
     * @return
     */
    public static String getHBaseContentTableCF(String task) {
        return "CONTENT_" + task.toUpperCase();
    }

    /**
     * 从oracle实时导出数据,导出记录的key
     * @param task
     * @return
     */
    public static String getRealTimeOracleRecordKey(String task) {
        return getOracleContentTableName(task).toLowerCase() + "-last_export_time";
    }

    /**
     * 字段名Json文件Bcp文件字段名数组对应的Key
     * @param task
     * @return
     */
    public static String getBcpColumnJsonKey(String task) {
        return "bcp-" + task.toLowerCase();
    }

    /**
     * 获取HDFS文件系统上HFile文件的临时存储目录
     * @param task
     * @return
     */
    public static String getHFileTaskDir(String task) {
        return "/tmp/hbase/hfile/" + task + "/";
    }


}
