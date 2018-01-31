package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * vid(虚拟)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class VidBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(VidBcpImport.class);

    //任务类型
    public static final String task = "vid";
    private static final long serialVersionUID = -6383618509896386301L;

    public static void main(String[] args) {
        while (true) {
            doTask(task);
        }
    }
}
