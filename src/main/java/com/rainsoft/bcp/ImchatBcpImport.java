package com.rainsoft.bcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * imchat(聊天)类型的Bcp数据导入到Solr、HBase
 * Created by CaoWeiDong on 2018-01-31.
 */
public class ImchatBcpImport extends BaseBcpImportHBaseSolr {
    public static final Logger logger = LoggerFactory.getLogger(ImchatBcpImport.class);

    //任务类型
    public static final String task = "im_chat";
    private static final long serialVersionUID = -5719196910421498635L;

    public static void main(String[] args) throws IOException {
        while (true) {
            doTask(task);
        }
    }
}
