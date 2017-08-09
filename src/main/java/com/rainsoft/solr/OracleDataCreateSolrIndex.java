package com.rainsoft.solr;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.dao.HttpDao;
import com.rainsoft.dao.ImchatDao;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;

/**
 * 功能说明：
 * 把Oracle的数据导入到Solr里，数据从后往前导入，
 * 比如说先导入2017-06-01的数据，再导入2017-05-31的数据
 * 有两个参数：
 * 开始时间
 * 结束时间
 * 数据按天导入，这一天的导完了再导前一天的，一直到结束时间
 * 如果导入失败的话尝试3次，如果都失败的话退出程序
 * <p>
 * 具体这样实现：
 * 把数据从Oracle查询出来后Solr一次处理所有的数据，这样的话Oracle一天的数据可能会有2G，
 * 但是经过各种封装后占的内存可能会有10G，不过反正内存够用。
 * <p>
 * 如果这样不行的话再按下面的方式实现：
 * 先从Oracle查一天的数据，查出来以后把数据写入到磁盘，按500万条数据一个文件，分批按文件处理，
 * Solr从磁盘拿数据，一次处理一个文件，如果这个任务失败了，重试3次，3次都失败了退出，
 * 下次再处理的时候先检查磁盘是否有数据，如果有的话先处理磁盘的数据，如果没有的话再从Oracle查询
 * <p>
 * Created by CaoWeiDong on 2017-06-12.
 */
public class OracleDataCreateSolrIndex {
    private static final Logger logger = LoggerFactory.getLogger(OracleDataCreateSolrIndex.class);

    //批量索引的数据量
    private static int dataFileLines = 1000000;
    //一次写入文件的数据量
    private static final int writeSize = 100000;
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    //数字输出格式
    private static NumberFormat numberFormat = NumberFormat.getNumberInstance();

    //创建Spring Context
    private static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    //ftpDao
    private static FtpDao ftpDao = (FtpDao) context.getBean("ftpDao");

    //httpDao
    private static HttpDao httpDao = (HttpDao) context.getBean("httpDao");

    //imChatDao
    private static ImchatDao imchatDao = (ImchatDao) context.getBean("imchatDao");

    //创建Solr客户端
    private static CloudSolrClient client = (CloudSolrClient) SolrUtil.getSolrClient(true);

    //导入记录文件
    private static File recordFile;

    //导入记录
    private static Map<String, String> recordMap = new HashMap<>();

    //创建模式：一次创建，多次创建
    private static String createMode;

    //字段间分隔符
    private static final String kvOutSeparator = "\\|;\\|";

    private static final String kvInnerSeparator = "\\|=\\|";

    private static final String FTP = "ftp";
    private static final String HTTP = "http";
    private static final String IMCHAT = "imchat";

    private static final String FTP_TYPE = "文件";
    private static final String HTTP_TYPE = "网页";
    private static final String IMCHAT_TYPE = "聊天";

    private static final String SUCCESS_STATUS = "success";
    private static final String FAIL_STATUS = "fail";

    public static void main(String[] args) throws IOException, SolrServerException, ParseException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        //开始日期
        Date startDate = DateUtils.parseDate(args[0], "yyyy-MM-dd");
        //结束日期
        Date endDate = DateUtils.parseDate(args[1], "yyyy-MM-dd");

        logger.info("参数 --> 开始日期：{}", args[0]);
        logger.info("参数 --> 结束日期：{}", args[1]);

        if (startDate.before(endDate)) {
            logger.error("开始导入日期参数必须大于等于结束导入日期参数");
            System.exit(0);
        } else if (startDate.equals(endDate)) {
            logger.info("开始时间等于结束时间,程序将导入{}一天的数据", args[0]);
        } else {
            logger.info("程序将导入从{}到{}的数据", args[1], args[0]);
        }

        createMode = args[2];
        if (args.length == 4) {
            dataFileLines = Integer.valueOf(args[3]);
        }

        logger.info("程序启动");
        long startTime = new Date().getTime();

        //数据导入结果的状态
        boolean importResultStatus = true;
        /*
         * 如果开始日期在结束日期的后面或者开始日期与结束日期是同一天，执行迁移
         * 从后向前按日期迁移数据，比如开始时间是2017-01-01，结束时间是2016-01-01
         * 第一次迁移的时间是：2017-01-01
         * 第二次迁移的时间是：2016-12-31
         * 。。。
         * 最后一次迁移的时间：2016-01-01
         */
        while ((startDate.after(endDate)) || (DateUtils.isSameDay(startDate, endDate))) {
            long startDayTime = new Date().getTime();
            //开始时间转为捕获时间参数
            String captureTime = com.rainsoft.utils.DateUtils.DATE_FORMAT.format(startDate);

            String ftpRecord = captureTime + "_" + FTP;
            //先检查前一次的导入,如果成功再执行此次导入,如果失败就跳出循环结束程序
            if (importResultStatus) {
                /*
                 * 导入FTP的数据，如果还没有导入或者导入失败，先删除当天的数据再执行导入程序
                 * 如果已经导入成功过了，则不再导入
                 */
                if (!SUCCESS_STATUS.equals(recordMap.get(ftpRecord))) {

                    //对当天的数据重新添加索引
                } else {
                    logger.info("{} : {} has already imported", captureTime, FTP);
                }
            } else {
                break;
            }

            String httpRecord = captureTime + "_" + HTTP;
            //先检查前一次的导入,如果成功再执行此次导入,如果失败就跳出循环结束程序
            if (importResultStatus) {
                /*
                 * 导入HTTP的数据，如果还没有导入或者导入失败，先删除当天的数据再执行导入程序
                 * 如果已经导入成功过了，则不再导入
                 */
                if (!SUCCESS_STATUS.equals(recordMap.get(httpRecord))) {


                } else {
                    logger.info("{} : {} has already imported", captureTime, HTTP);
                }
            } else {
                break;
            }

            String imchatRecord = captureTime + "_" + IMCHAT;
            //先检查前一次的导入,如果成功再执行此次导入,如果失败就跳出循环结束程序
            if (importResultStatus) {
                /*
                 * 导入FTP的数据，如果还没有导入或者导入失败，且前一次任务导入成功则执行导入程序
                 * 如果已经导入成功过了，则不再导入
                 */
                if (!SUCCESS_STATUS.equals(recordMap.get(imchatRecord))) {

                    //对当天的数据重新添加索引
                } else {
                    logger.info("{} : {} has already imported", captureTime, IMCHAT);
                }
            } else {
                break;
            }

            //开始时间减少一天
            startDate = DateUtils.addDays(startDate, -1);
            logger.info("{} 数据索引完毕", captureTime);

            long endDayTime = new Date().getTime();
            long runTime = (endDayTime - startDayTime) / 1000;

            logger.info("索引{}天的数据,程序执行时间: {}分钟{}秒", captureTime, runTime / 60, runTime % 60);
        }

        logger.info("执行完毕");
        long endTime = new Date().getTime();

        long totalRunTime = (endTime - startTime) / 1000;
        logger.info("程序总共执行时间: {}分钟{}秒", totalRunTime / 60, totalRunTime % 60);
        context.close();
        System.exit(0);

    }

}
