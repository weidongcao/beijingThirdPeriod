package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.BcpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  将BCP文件转为TSV文件
 * Created by CaoWeiDong on 2017-08-09.
 */
public class TransformBcp2Tsv {
    private static final Logger logger = LoggerFactory.getLogger(TransformBcp2Tsv.class);
    public static void main(String[] args) {

        //任务类型(ftp/http/im_chat)
        String taskType = args[0].toLowerCase();
        //BCP文件所在目录格式
        String resourcePathTemplate = "${bcp_file_path}/${task}";
        //TSV文件所在目录格式
        String targetPathTemplate = "${load_data_workspace}/work/bcp-${task}";

        //BCP文件目录
        String resourcePath = resourcePathTemplate.replace("${task}", taskType)
                .replace("${bcp_file_path}", ConfigurationManager.getProperty("bcp.file.path"));

        //TSV文件目录
        String targetPath = targetPathTemplate.replace("${task}", taskType)
                .replace("${load_data_workspace}", ConfigurationManager.getProperty("load.data.workspace"));

        logger.info("开始将BCP文件转转换为TSV文件:{}", taskType);
        
        /*
         * BcpUtils方法参数说明：
         *  resourcePath:BCP文件所在目录
         *  targetPath:TSV文件存储目录
         *  task:任务类型(ftp/http/im_chat)生成TSV文件命令用
         *  captureTimeIndex    捕获时间是第几个字段(从0开始)
         */
        if (BigDataConstants.CONTENT_TYPE_FTP.equals(taskType)) {
            BcpUtils.transformBcpToTsv(resourcePath, targetPath, taskType, 17);
        } else if (BigDataConstants.CONTENT_TYPE_IM_CHAT.equals(taskType)) {
            BcpUtils.transformBcpToTsv(resourcePath, targetPath, taskType, 20);
        } else if (BigDataConstants.CONTENT_TYPE_HTTP.equals(taskType)) {
            BcpUtils.transformBcpToTsv(resourcePath, targetPath, taskType, 22);
        }
        logger.info("转换完成：{}", taskType);

    }
}
