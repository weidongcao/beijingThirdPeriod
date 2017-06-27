package com.rainsoft.solr;

import com.rainsoft.dao.ImchatDao;
import com.rainsoft.domain.RegContentImChat;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class ImchatOracleDataCreateSolrIndex extends BaseOracleDataCreateSolrIndex {
    private static final Logger logger = LoggerFactory.getLogger(FtpOracleDataCreateSolrIndex.class);

    //imChatDao
    protected static ImchatDao imchatDao = (ImchatDao) context.getBean("imchatDao");

    private static boolean imChatCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        logger.info("imChat : 开始索引 {} 的数据", captureTime);

        //获取数据库一天的数据
        List<RegContentImChat> dataList = imchatDao.getImchatBydate(captureTime);

        boolean flat = imChatCreateIndex(dataList, client);

        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_" + IMCHAT, SUCCESS_STATUS);
        } else {
            recordMap.put(captureTime + "_" + IMCHAT, FAIL_STATUS);
            logger.error("当天数据导入失败");
        }

        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        Collections.sort(newRecordList);

        String newRecords = StringUtils.join(newRecordList, "\r\n");

        FileUtils.writeStringToFile(recordFile, newRecords, false);

        logger.info("imchat : {} 的数据,索引完成", captureTime);
        return flat;
    }
    private static boolean imChatCreateIndex(List<RegContentImChat> dataList, SolrClient client) throws IOException, SolrServerException {
        logger.info("当前要索引的数据量 = " + numberFormat.format(dataList.size()));
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
            List<RegContentImChat> sublist;
            if (writeSize < dataList.size()) {
                sublist = dataList.subList(0, writeSize);
            } else {
                sublist = dataList;
            }

             /*
             * 将Oracle查询出来的数据封装为Solr的导入实体
             */
            for (RegContentImChat imChat : sublist) {
                //创建SolrInputDocument实体
                SolrInputDocument doc = new SolrInputDocument();

                String uuid = UUID.randomUUID().toString().replace("-", "");
                imChat.setId(uuid);

                doc.addField("docType", IMCHAT_TYPE);

                //数据实体属性集合
                Field[] fields = RegContentImChat.class.getFields();

                //生成Solr导入实体
                for (Field field : fields) {
                    String fieldName = field.getName();
                    doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, imChat));
                }
                //导入时间
                doc.addField("IMPORT_TIME", com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()));
                try {
                    doc.addField("capture_time", com.rainsoft.utils.DateUtils.TIME_FORMAT.parse(imChat.getCapture_time().split("\\.")[0]).getTime());
                } catch (Exception e) {
                    logger.info("聊天采集时间转换失败,采集时间为： {}", imChat.getCapture_time());
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

            logger.info("第 {} 次索引10万条数据成功;剩余未索引的数据: {} 条", submitCount, numberFormat.format(dataList.size()));
        }

        long endIndexTime = new Date().getTime();
        //计算索引一天的数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;
        logger.info("IMCHAT索引一天的数据执行时间: {}分钟{}秒", indexRunTime / 60, indexRunTime % 60);

        return flat;
    }

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, SolrServerException, IOException {
        imChatCreateSolrIndexByDay(args[0]);
    }
}
