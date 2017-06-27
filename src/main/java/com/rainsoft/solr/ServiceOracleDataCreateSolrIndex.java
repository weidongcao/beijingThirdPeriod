package com.rainsoft.solr;

import com.rainsoft.dao.ServiceDao;
import com.rainsoft.domain.ServiceInfo;
import com.rainsoft.utils.ReflectUtils;
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
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ServiceOracleDataCreateSolrIndex extends BaseOracleDataCreateSolrIndex {
    private static final Logger logger = LoggerFactory.getLogger(ServiceOracleDataCreateSolrIndex.class);
    private static ServiceDao serviceDao = (ServiceDao) context.getBean("serviceDao");

    public static void main(String[] args) throws IllegalAccessException, InvocationTargetException, ParseException, IOException, NoSuchMethodException, SolrServerException {
        serviceCreateSolrIndexByDay("2017-06-28");
    }
    private static boolean serviceCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ParseException {
        logger.info("service : 开始索引 {} 的数据", captureTime);

        //获取数据库一天的数据
        List<ServiceInfo> dataList = serviceDao.getServiceByPeriod(captureTime);

        logger.info("从数据库查询数据结束");
        boolean flat = serviceCreateIndex(dataList, client);

        logger.info("service : {} 的数据,索引完成", captureTime);
        return flat;
    }

    private static boolean serviceCreateIndex(List<ServiceInfo> dataList, SolrClient client) throws IOException, SolrServerException {
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
            List<ServiceInfo> sublist;
            if (writeSize < dataList.size()) {
                sublist = dataList.subList(0, writeSize);
            } else {
                sublist = dataList;
            }

            /*
             * 将Oracle查询出来的数据封装为Solr的导入实体
             */
            for (ServiceInfo service : sublist) {
                //创建SolrInputDocument实体
                SolrInputDocument doc = new SolrInputDocument();

                //生成Solr的唯一ID
                String uuid = UUID.randomUUID().toString().replace("-", "");
                doc.addField("ID", uuid);

                //添加FTP数据类型为文件
                doc.addField("docType", SERVICE_TYPE);

                //数据实体属性集合
                Field[] fields = ServiceInfo.class.getFields();

                //遍历实体属性,将之赋值给Solr导入实体
                for (Field field : fields) {
                    String fieldName = field.getName();
                    doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, service));
                }
                //导入时间
                doc.addField("IMPORT_TIME", com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()));

                //索引实体添加缓冲区
                cacheList.add(doc);
            }

            //索引到Solr
            flat = OracleDataCreateSolrIndex.submitSolr(cacheList, client);

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
        //计算索引数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;

        logger.info("service索引数据执行时间: {}分钟{}秒", indexRunTime / 60, indexRunTime % 60);

        return flat;
    }

}
