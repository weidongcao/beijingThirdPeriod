package com.rainsoft.bcp;

import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * FTP类型的BCP文件导入到SOlr、HBase
 * Created by CaoWeiDong on 2017-10-20.
 */
class FtpBcpImportHBaseSolr extends BaseBcpImportHBaseSolr {
    private static final Logger logger = LoggerFactory.getLogger(FtpBcpImportHBaseSolr.class);

    private static final String task = "ftp";

    public static void bcpImportHBaseSolr(){
        logger.info("开始处理 {} 类型的Bcp文件", task);
        //将Bcp文件从文件池中移到工作目录
        moveBcpfileToWorkDir(task);

        // Bcp文件导入到HBase、Solr
        filesContentImportHBaseSolr(task);
    }
}
