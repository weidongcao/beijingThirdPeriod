package com.rainsoft.solr;

import com.rainsoft.dao.HttpDao;
import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.utils.ReflectUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class HttpOracleDataCreateSolrIndex extends BaseOracleDataCreateSolrIndex {
    private static final Logger logger = LoggerFactory.getLogger(FtpOracleDataCreateSolrIndex.class);

    //httpDao
    protected static HttpDao httpDao = (HttpDao) context.getBean("httpDao");

    private static boolean httpCreateSolrIndexByDay(String captureTime, float startPercent, float endPercent) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        logger.info("http : 开始索引 {} 的数据", captureTime);

        //获取数据库一天的数据
        List<RegContentHttp> dataList = httpDao.getHttpBydate(captureTime, startPercent, endPercent);

        logger.info("从数据库查询结束...");

        boolean flat = httpCreateIndex(dataList, client);

        if (endPercent == 1) {
            //数据索引结果成功或者失败写入记录文件,
            if (flat) {
                recordMap.put(captureTime + "_" + HTTP, SUCCESS_STATUS);
            } else {
                recordMap.put(captureTime + "_" + HTTP, FAIL_STATUS);
                logger.info("当天数据导入失败");
            }

            List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

            Collections.sort(newRecordList);

            String newRecords = StringUtils.join(newRecordList, "\r\n");

            FileUtils.writeStringToFile(recordFile, newRecords, false);

            logger.info("http : {} 的数据,索引完成", captureTime);
        }

        return flat;
    }

    private static boolean httpCreateIndex(List<RegContentHttp> dataList, SolrClient client) throws IOException, SolrServerException {
        logger.info("当前要索引的数据量 = {}", numberFormat.format(dataList.size()));
        long startIndexTime = new Date().getTime();

        //缓冲数据list
        List<SolrInputDocument> cacheList = new ArrayList<>();

        //数据索引结果状态
        boolean flat = true;

        int submitCount = 0;
        //进行Solr索引
        while (dataList.size() > 0) {
            /*
             * 从Oracle查询出来的数据量很大,每次从查询出来的List中取一定量的数据进行索引
             */
            List<RegContentHttp> sublist;
            if (writeSize < dataList.size()) {
                sublist = dataList.subList(0, writeSize);
            } else {
                sublist = dataList;
            }

            /*
             * 将Oracle查询出来的数据封装为Solr的导入实体
             */
            for (RegContentHttp http : sublist) {
                //创建SolrImputDocument实体
                SolrInputDocument doc = new SolrInputDocument();

                String uuid = UUID.randomUUID().toString().replace("-", "");
                doc.addField("ID", uuid);

                doc.addField("docType", HTTP_TYPE);

                //数据实体属性集合
                Field[] fields = RegContentHttp.class.getFields();

                //遍历实体属性,将之赋值给Solr导入实体
                addFieldToSolr(doc, fields, http);

                try {
                    doc.addField("capture_time", com.rainsoft.utils.DateUtils.TIME_FORMAT.parse(http.getCapture_time().split("\\.")[0]).getTime());
                } catch (ParseException e) {
                    logger.error("HTTP采集时间转换失败,采集时间为： {}", http.getCapture_time());
                    e.printStackTrace();
                }

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

            //移除已经索引过的数据
            if (writeSize < dataList.size()) {
                dataList = dataList.subList(writeSize, dataList.size());
            } else {
                dataList.clear();
            }

            logger.info("第 {} 次索引10万条数据成功;剩余未索引的数据: {}条", submitCount, numberFormat.format(dataList.size()));
        }

        long endIndexTime = new Date().getTime();
        //计算索引一天的数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;
        logger.info("HTTP索引数据执行时间: {}分钟{}秒", indexRunTime / 60, indexRunTime % 60);

        return flat;
    }
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, SolrServerException, IOException {
        String date = args[0];
        float startPercent = Float.valueOf(args[1]);
        float endPercent = Float.valueOf(args[2]);
        String httpRecord = date + "_" + HTTP;
        if (!SUCCESS_STATUS.equals(recordMap.get(httpRecord))) {
            httpCreateSolrIndexByDay(date, startPercent, endPercent);
        } else {
            logger.info("{} : {} has already imported", date, HTTP);
        }
        client.close();

    }

}
