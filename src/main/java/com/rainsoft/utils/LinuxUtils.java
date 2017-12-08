package com.rainsoft.utils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;

/**
 * Created by CaoWeiDong on 2017-12-08.
 */
public class LinuxUtils {
    public static final Logger logger = LoggerFactory.getLogger(LinuxUtils.class);

    //将云探的Bcp文件从文件池中移到工作目录命令模板
    public static final String SHELL_YUNTAN_BCP_MV = "find ${bcp_pool_dir} -name \"*-${task}*.bcp\"  | tail -n ${operator_bcp_number} |xargs -i mv {} ${bcp_file_path}/${task}";
    /**
     * Java执行外部Shell命令
     *
     * @param task    任务类型
     * @param shellMv 要执行的Shell命令
     */
    public static void execShell(String task, String shellMv) {
        //执行Shell命令,将Bcp文件从文件池移动到工作目录
        try {
            logger.info("执行Shell命令,将 {} 类型的Bcp文件从文件池移动到工作目录", task);
            logger.info("执行 Shell命令:{}", shellMv);
            Process p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", shellMv});
            int exitVal = p.waitFor();
            if (exitVal != 0) {
                BufferedInputStream in = new BufferedInputStream(p.getErrorStream());
                logger.error("执行 {} 失败:{}", shellMv, IOUtils.toString(in, "utf-8"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("执行 Shell命令失败:{}", shellMv);
            System.exit(-1);
        }
    }
}
