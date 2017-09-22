package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.BcpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TransformBcp2Tsv功能的升级
 *  将BCP文件转为TSV文件
 *  并对关键字段进行过滤
 * Created by CaoWeiDong on 2017-08-09.
 */
public class TransformBcp2Tsv2 {
    private static final Logger logger = LoggerFactory.getLogger(TransformBcp2Tsv2.class);
    public static void main(String[] args) {

        //任务类型(ftp/http/im_chat)
        String taskType = args[0].toLowerCase();
        //BCP文件所在目录格式
        String resourcePathTemplate = "${bcp_file_path}/${taskType}";
        //TSV文件所在目录格式
        String targetPathTemplate = "${load_data_workspace}/work/bcp-${taskType}";

        //BCP文件目录
        String resourcePath = resourcePathTemplate.replace("${taskType}", taskType)
                .replace("${bcp_file_path}", ConfigurationManager.getProperty("bcp_file_path"));

        //TSV文件目录
        String targetPath = targetPathTemplate.replace("${taskType}", taskType)
                .replace("${load_data_workspace}", ConfigurationManager.getProperty("load_data_workspace"));

        logger.info("开始将BCP文件转转换为TSV文件:{}", taskType);
        
        /*
         *  resourcePath:BCP文件所在目录
         *  targetPath:TSV文件存储目录
         */
        if (BigDataConstants.CONTENT_TYPE_FTP.equals(taskType)) {
            new FtpBcpTransform2Tsv().transformBcpToTsv(resourcePath, targetPath);
        } else if (BigDataConstants.CONTENT_TYPE_IM_CHAT.equals(taskType)) {
            new ImchatBcpTransform2Tsv().transformBcpToTsv(resourcePath, targetPath);
        } else if (BigDataConstants.CONTENT_TYPE_HTTP.equals(taskType)) {
            new HttpBcpTransform2Tsv().transformBcpToTsv(resourcePath, targetPath);
        }
        logger.info("转换完成：{}", taskType);

    }
}
