package com.rainsoft.solr;

import com.rainsoft.dao.RealDao;
import com.rainsoft.domain.RegRealIdInfo;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Oracle数据库 真实 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RealOracleDataCreateSolrIndex extends BaseOracleDataCreateSolrIndex {
    private static final Logger logger = LoggerFactory.getLogger(RealOracleDataCreateSolrIndex.class);

    private static RealDao realDao = (RealDao) context.getBean("realDao");

    private static final String REAL = "real";
    private static final String REAL_TYPE = "真实";

    public static void main(String[] args) throws IllegalAccessException, InvocationTargetException, ParseException, IOException, NoSuchMethodException, SolrServerException {
        String date = args[0];

        String ftpRecord = date + "_" + REAL;
        if (!SUCCESS_STATUS.equals(recordMap.get(ftpRecord))) {
            realCreateSolrIndexByDay(args[0]);
            //对当天的数据重新添加索引
        } else {
            logger.info("{} : {} has already imported", date, REAL);
        }

        client.close();
    }

    private static boolean realCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ParseException {
        logger.info("执行的Shell命令： java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.RealOracleDataCreateSolrIndex {}", captureTime);

        logger.info("real : 开始索引 {} 的数据", captureTime);

        //获取数据库一天的数据
        List<RegRealIdInfo> dataList = realDao.getRealByPeriod(captureTime);

        logger.info("从数据库查询数据结束");
        boolean flat = realCreateIndex(dataList, client);

        //导入完成后对不同的结果的处理
        recordImportResult(REAL, captureTime, flat);

        return flat;
    }

    private static boolean realCreateIndex(List<RegRealIdInfo> dataList, SolrClient client) throws IOException, SolrServerException {
        logger.info("当前要索引的数据量 = {}", numberFormat.format(dataList.size()));
        long startIndexTime = new Date().getTime();

        //缓冲数据
        List<SolrInputDocument> cacheList = new ArrayList<>();

        //数据索引结果状态
        boolean flat = true;

        int submitCount = 0;
        //进行Solr索引
        while (dataList.size() > 0) {
            /*
             * 从Oracle查询出来的数据量很大,每次从查询出来的List中取一定量的数据进行索引
             */
            List<RegRealIdInfo> sublist;
            if (writeSize < dataList.size()) {
                sublist = dataList.subList(0, writeSize);
            } else {
                sublist = dataList;
            }

            /*
             * 将Oracle查询出来的数据封装为Solr的导入实体
             */
            for (RegRealIdInfo real : sublist) {
                //创建SolrInputDocument实体
                SolrInputDocument doc = new SolrInputDocument();

                //生成Solr的唯一ID
                String uuid = UUID.randomUUID().toString().replace("-", "");
                doc.addField("ID", uuid);

                //添加FTP数据类型为文件
                doc.addField("docType", REAL_TYPE);

                //数据实体属性集合
                Field[] fields = RegRealIdInfo.class.getFields();

                //遍历实体属性,将之赋值给Solr导入实体
                addFieldToSolr(doc, fields, real);

                //索引实体添加缓冲区
                cacheList.add(doc);
            }

            //索引到Solr
            flat = submitSolr(cacheList, client);

            //有一次索引失败就认为失败
            if (!flat) {
                return flat;
            }

            //清空历史索引数据
            cacheList.clear();

            //提交次数增加
            submitCount++;

            int tempSubListSize = sublist.size();
            //移除已经索引过的数据
            if (writeSize < dataList.size()) {
                dataList = dataList.subList(writeSize, dataList.size());
            } else {
                dataList.clear();
            }
            logger.info("第 {} 次索引 {} 条数据成功;剩余未索引的数据: {}条", submitCount, numberFormat.format(tempSubListSize), numberFormat.format(dataList.size()));
        }

        long endIndexTime = new Date().getTime();
        //计算索引数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;

        logger.info("real索引数据执行时间: {}分钟{}秒", indexRunTime / 60, indexRunTime % 60);

        return flat;
    }

}
