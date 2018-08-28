package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 云采
 * ending_track类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2017-12-08.
 */
public class EndingTrackBcpImport extends BaseBcpImportHBaseSolr {

    private static final long serialVersionUID = 469839933566133855L;
    public static final Logger logger = LoggerFactory.getLogger(EndingTrackBcpImport.class);

    //任务类型
    public static final String task = "ending_trace";

    public static void main(String[] args) throws IOException {
        while (true) {
            doTask(task);
        }
    }
}
