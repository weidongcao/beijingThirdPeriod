package com.rainsoft.bcp;

import com.rainsoft.utils.NamingRuleUtils;
import com.rainsoft.utils.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by CaoWeiDong on 2017-10-20.
 */
public class FtpBcpImportHBaseSolr extends BaseBcpImportHBaseSolr {
    private static final Logger logger = LoggerFactory.getLogger(FtpBcpImportHBaseSolr.class);

    private static final String task = "ftp";

    public void bcpImportHBaseSolr(){
        logger.info("开始处理 {} 类型的Bcp文件", task);
        //将Bcp文件从文件池中移到工作目录
        moveBcpfileToWorkDir(task);

        //BCP文件所在目录
        File dir = FileUtils.getFile(NamingRuleUtils.getBcpWorkDir(task));
        //BCP文件列表
        File[] files = dir.listFiles();

        //遍历BCP文件,数据导入Solr、HBase
        assert files != null;
        for (File file : files) {
            List<String> lines = null;
            try {
                lines = FileUtils.readLines(file);
                runImport(lines, task);
                //删除文件
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("读取文件失败：file={}, error={}", file.getAbsolutePath(), e);
                //执行失败把此文件移动出去
                file.renameTo(new File("/opt/bcp/error", file.getName()));
            }
        }
    }
}
