package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * search(搜索)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class SearchBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(SearchBcpImport.class);

    //任务类型
    public static final String task = "search";
    private static final long serialVersionUID = 1225764361781766264L;

    public static void main(String[] args) {
        while (true) {
            doTask(task);
            break;
        }
    }
}
