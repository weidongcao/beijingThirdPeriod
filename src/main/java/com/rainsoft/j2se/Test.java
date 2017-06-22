package com.rainsoft.j2se;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 2017-06-22.
 */
public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) {
        logger.error("[info message]");
        logger.debug("[debug message]");
        logger.trace("[trace message]");
        List<Integer> list = new ArrayList();
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            list.add(random.nextInt(900) + 100);
        }

        if (list.isEmpty() == false) {
            /**
             * 每10万条数据提交一次
             */
            int batchSize = 3;
            List<Integer> sublist;
            while (batchSize < list.size()) {
                sublist = list.subList(0, batchSize);
//                System.out.println("sublist = " + StringUtils.join(sublist));
                System.out.println(sublist.toString());
                list = list.subList(batchSize, list.size());
            }

            if (list.size() > 0) {
//                System.out.println("list = " + StringUtils.join(list));
                System.out.println(list.toString());
            }
        }
        logger.info("alskdjflsadjfk");
    }
}
