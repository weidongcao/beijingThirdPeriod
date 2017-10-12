package com.rainsoft.solr;

import com.rainsoft.dao.ShopDao;
import com.rainsoft.domain.RegContentShop;
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
 * Oracle数据库 shop 数据导入Solr
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ShopOracleDataExport extends BaseOracleDataExport {
    private static final Logger logger = LoggerFactory.getLogger(ShopOracleDataExport.class);

    private static ShopDao shopDao = (ShopDao) context.getBean("shopDao");

    private static final String SHOP = "shop";
    private static final String SHOP_TYPE = "SHOP";

    public static void main(String[] args) throws IllegalAccessException, InvocationTargetException, ParseException, IOException, NoSuchMethodException, SolrServerException {
        long startIndexTime = new Date().getTime();

        String date = args[0];
        String ftpRecord = date + "_" + SHOP;
        if (!SUCCESS_STATUS.equals(recordMap.get(ftpRecord))) {
            shopCreateSolrIndexByDay(args[0]);
            //对当天的数据重新添加索引
        } else {
            logger.info("{} : {} has already imported", date, SHOP);
        }
        long endIndexTime = new Date().getTime();
        //计算索引数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;
        logger.info("shop索引数据执行时间: {}分钟{}秒", indexRunTime / 60, indexRunTime % 60);

        client.close();
    }

    private static boolean shopCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ParseException {
        logger.info("执行的Shell命令： java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ShopOracleDataExport {}", captureTime);

        logger.info("shop : 开始索引 {} 的数据", captureTime);

        //获取数据库一天的数据
        List<RegContentShop> dataList = shopDao.getShopByPeriod(captureTime);

        logger.info("从数据库查询数据结束");
        boolean flat = shopCreateIndex(dataList, client);

        //导入完成后对不同的结果的处理
        recordImportResult(SHOP, captureTime, flat);

        return flat;
    }

    private static boolean shopCreateIndex(List<RegContentShop> dataList, SolrClient client) throws IOException, SolrServerException {
        logger.info("当前要索引的数据量 = {}", numberFormat.format(dataList.size()));

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
            List<RegContentShop> sublist;
            if (writeSize < dataList.size()) {
                sublist = dataList.subList(0, writeSize);
            } else {
                sublist = dataList;
            }

            /*
             * 将Oracle查询出来的数据封装为Solr的导入实体
             */
            for (RegContentShop shop : sublist) {
                //创建SolrInputDocument实体
                SolrInputDocument doc = new SolrInputDocument();

                //生成Solr的唯一ID
                String uuid = UUID.randomUUID().toString().replace("-", "");
                doc.addField("ID", uuid);

                //添加FTP数据类型为文件
                doc.addField("docType", SHOP_TYPE);

                //数据实体属性集合
                Field[] fields = RegContentShop.class.getFields();

                //遍历实体属性,将之赋值给Solr导入实体
//                addFieldToSolr(doc, fields, shop);

                //捕获时间转为毫秒赋值给Solr导入实体
                try {
                    doc.addField("capture_time", com.rainsoft.utils.DateUtils.TIME_FORMAT.parse(shop.getCapture_time().split("\\.")[0]).getTime());
                } catch (Exception e) {
                    logger.info("shop采集时间转换失败,采集时间为： {}", shop.getCapture_time());
                    e.printStackTrace();
                }

                //索引实体添加缓冲区
                cacheList.add(doc);
            }

            //索引到Solr
//            flat = submitSolr(cacheList, client);

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


        return flat;
    }

}