package com.rainsoft.solr;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.dao.HttpDao;
import com.rainsoft.dao.ImchatDao;
import com.rainsoft.domain.RegContentFtp;
import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.domain.RegContentImChat;
import com.rainsoft.utils.ReflectUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

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
    //批量索引的数据量
    private static int batchCount = 100000;
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    //创建Spring Context
    private static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    //创建Solr客户端
    private static CloudSolrClient client = SolrUtil.getSolrClient("yisou");

    //导入记录文件
    private static File recordFile;

    //导入记录
    private static Map<String, String> recordMap = new HashMap<>();

    static {
        //导入记录
        String importRecordFile = "createIndexRecord/index-record.txt";
        //转换文件分隔符,使在Window和Linux下都可以使用
        String convertImportRecordFile = importRecordFile.replace("/", FILE_SEPARATOR).replace("\\", FILE_SEPARATOR);
        //创建导入记录文件
        recordFile = FileUtils.getFile(convertImportRecordFile);
        File parentFile = recordFile.getParentFile();

        if (parentFile.exists() == false) {
            parentFile.mkdirs();
        }

        if (recordFile.exists() == false) {
            try {
                recordFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //导入记录
        List<String> recordsList = null;
        try {
            recordsList = FileUtils.readLines(recordFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //导入记录转为Map
        for (String record : recordsList) {
            String[] kv = record.split("\t");
            recordMap.put(kv[0], kv[1]);
        }
    }

    public static void main(String[] args) throws IOException, SolrServerException, ParseException, NoSuchFieldException {

        //数据类型
        String dataType = args[0];
        //开始日期
        Date startDate = DateUtils.parseDate(args[1], "yyyy-MM-dd");
        //结束日期
        Date endDate = DateUtils.parseDate(args[2], "yyyy-MM-dd");

        if (dataType.equals("ftp")) {
            CreateFtpSolrIndex(dataType, startDate, endDate);
        } else if (dataType.equals("http")) {
            CreateHttpSolrIndex(dataType, startDate, endDate);
        } else if (dataType.equals("imchat")) {
            OracleDataCreateImchatSolrIndex(dataType, startDate, endDate);
        }


        context.close();
        System.exit(0);

    }

    private static void OracleDataCreateImchatSolrIndex(String dataType, Date startDate, Date endDate) throws IOException, SolrServerException {
        //装配FtpDao
        ImchatDao imchatDao = (ImchatDao) context.getBean("imchatDao");
        /**
         * 如果开始日期在结束日期的后面或者开始日期与结束日期是同一天，执行迁移
         * 从后向前按日期迁移数据，比如开始时间是2017-01-01，结束时间是2016-01-01
         * 第一次迁移的时间是：2017-01-01
         * 第二次迁移的时间是：2016-12-31
         * 。。。
         * 最后一次迁移的时间：2016-01-01
         */
        while ((startDate.after(endDate)) || (DateUtils.isSameDay(startDate, endDate))) {
            //开始时间转为捕获时间参数
            String captureTime = DateFormatUtils.ISO_DATE_FORMAT.format(startDate);

            //获取数据库一天的数据
            List<RegContentImChat> datalist = imchatDao.getImchatBydate(captureTime);
            //对这一天的数据进行索引
            boolean importStatus = CreateImchatDataSolrIndexByDay(datalist, client);

            //数据索引结果成功或者失败写入记录文件,
            String record;
            if (importStatus) {
                recordMap.put(captureTime + "_" + dataType, "success");
            } else {
                recordMap.put(captureTime + "_" + dataType, "fail");
            }

            List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

            Collections.sort(newRecordList);

            String newRecords = StringUtils.join(newRecordList, "\r\n");

            FileUtils.writeStringToFile(recordFile, newRecords, false);

            //开始时间减少一天
            startDate = DateUtils.addDays(startDate, -1);
        }
    }

    private static void CreateHttpSolrIndex(String dataType, Date startDate, Date endDate) throws IOException, SolrServerException {
        //装配FtpDao
        HttpDao httpDao = (HttpDao) context.getBean("httpDao");
        /**
         * 如果开始日期在结束日期的后面或者开始日期与结束日期是同一天，执行迁移
         * 从后向前按日期迁移数据，比如开始时间是2017-01-01，结束时间是2016-01-01
         * 第一次迁移的时间是：2017-01-01
         * 第二次迁移的时间是：2016-12-31
         * 。。。
         * 最后一次迁移的时间：2016-01-01
         */
        while ((startDate.after(endDate)) || (DateUtils.isSameDay(startDate, endDate))) {
            //开始时间转为捕获时间参数
            String captureTime = DateFormatUtils.ISO_DATE_FORMAT.format(startDate);

            //获取数据库一天的数据
            List<RegContentHttp> datalist = httpDao.getHttpBydate(captureTime);
//            System.out.println(gson.toJson(datalist));
            //对这一天的数据进行索引
            boolean importStatus = CreateHttpDataSolrIndexByDay(datalist, client);

            //数据索引结果成功或者失败写入记录文件,
            String record;
            if (importStatus) {
                recordMap.put(captureTime + "_" + dataType, "success");
            } else {
                recordMap.put(captureTime + "_" + dataType, "fail");
            }

            List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

            Collections.sort(newRecordList);

            String newRecords = StringUtils.join(newRecordList, "\r\n");

            FileUtils.writeStringToFile(recordFile, newRecords, false);

            //开始时间减少一天
            startDate = DateUtils.addDays(startDate, -1);
        }
    }

    public static void CreateFtpSolrIndex(String dataType, Date startDate, Date endDate) throws IOException, SolrServerException {
        //装配FtpDao
        FtpDao ftpDao = (FtpDao) context.getBean("ftpDao");
        /**
         * 如果开始日期在结束日期的后面或者开始日期与结束日期是同一天，执行迁移
         * 从后向前按日期迁移数据，比如开始时间是2017-01-01，结束时间是2016-01-01
         * 第一次迁移的时间是：2017-01-01
         * 第二次迁移的时间是：2016-12-31
         * 。。。
         * 最后一次迁移的时间：2016-01-01
         */
        while ((startDate.after(endDate)) || (DateUtils.isSameDay(startDate, endDate))) {
            //开始时间转为捕获时间参数
            String captureTime = DateFormatUtils.ISO_DATE_FORMAT.format(startDate);

            //获取数据库一天的数据
            List<RegContentFtp> datalist = ftpDao.getFtpBydate(captureTime);
//            System.out.println(gson.toJson(datalist));
            //对这一天的数据进行索引
            boolean importStatus = CreateFtpDataSolrIndexByDay(datalist, client);

            //数据索引结果成功或者失败写入记录文件,
            String record;
            if (importStatus) {
                recordMap.put(captureTime + "_" + dataType, "success");
            } else {
                recordMap.put(captureTime + "_" + dataType, "fail");
            }

            List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

            Collections.sort(newRecordList);

            String newRecords = StringUtils.join(newRecordList, "\r\n");

            FileUtils.writeStringToFile(recordFile, newRecords, false);

            //开始时间减少一天
            startDate = DateUtils.addDays(startDate, -1);
        }
    }
    public static boolean CreateImchatDataSolrIndexByDay(List<RegContentImChat> datalist, SolrClient client) throws IOException, SolrServerException {
        //缓冲数据
        List<SolrInputDocument> cacheList = new ArrayList<>();

        int dataCount = 0;
        //进行Solr索引
        for (RegContentImChat imChat : datalist) {
            //创建SolrImputDocument实体
            SolrInputDocument doc = new SolrInputDocument();

            String uuid = UUID.randomUUID().toString().replace("-", "");

            doc.addField("ID", uuid);
            doc.addField("docType", "聊天");

            //数据实体属性集合
            Field[] fields = RegContentImChat.class.getFields();

            //生成Solr导入实体
            for (Field field : fields) {
                String fieldName = field.getName();
                if (fieldName.equalsIgnoreCase("id") == false) {
                    doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, imChat));
                }
            }

            //索引实体添加缓冲区
            cacheList.add(doc);
            dataCount++;
        }
        System.out.println("dataCount = " + dataCount);

        return submitSolr(cacheList, client);
    }
    public static boolean CreateHttpDataSolrIndexByDay(List<RegContentHttp> datalist, SolrClient client) throws IOException, SolrServerException {
        //缓冲数据
        List<SolrInputDocument> cacheList = new ArrayList<>();

        int dataCount = 0;
        //进行Solr索引
        for (RegContentHttp http : datalist) {
            //创建SolrImputDocument实体
            SolrInputDocument doc = new SolrInputDocument();

            String uuid = UUID.randomUUID().toString().replace("-", "");

            doc.addField("ID", uuid);
            doc.addField("docType", "网页");

            //数据实体属性集合
            Field[] fields = RegContentHttp.class.getFields();

            //生成Solr导入实体
            for (Field field : fields) {
                String fieldName = field.getName();
                if (fieldName.equalsIgnoreCase("id") == false) {
                    doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, http));
                }
            }

            //索引实体添加缓冲区
            cacheList.add(doc);
            dataCount++;
        }
        System.out.println("dataCount = " + dataCount);

        return submitSolr(cacheList, client);
    }

    public static boolean CreateFtpDataSolrIndexByDay(List<RegContentFtp> datalist, SolrClient client) throws IOException, SolrServerException {
        //缓冲数据
        List<SolrInputDocument> cacheList = new ArrayList<>();

        int dataCount = 0;
        //进行Solr索引
        for (RegContentFtp ftp : datalist) {
            //创建SolrImputDocument实体
            SolrInputDocument doc = new SolrInputDocument();

            String uuid = UUID.randomUUID().toString().replace("-", "");

            doc.addField("ID", uuid);
            doc.addField("docType", "文件");

            //数据实体属性集合
            Field[] fields = RegContentFtp.class.getFields();

            //生成Solr导入实体
            for (Field field : fields) {
                String fieldName = field.getName();
                if (fieldName.equalsIgnoreCase("id") == false) {
                    doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, ftp));
                }
            }

            //索引实体添加缓冲区
            cacheList.add(doc);
            dataCount++;
        }
        System.out.println("dataCount = " + dataCount);

        return submitSolr(cacheList, client);
    }

    /**
     * 提交导入Solr索引
     * @param cacheList 要导入Solr的索引
     * @param client    SolrClient
     * @return
     */
    public static boolean submitSolr(List<SolrInputDocument> cacheList, SolrClient client) {
        /**
         * 异常捕获
         * 如果失败尝试3次
         */
        int tryCount = 0;
        while (tryCount < 3) {
            try {
                if (cacheList.isEmpty() == false) {
                    client.add(cacheList, 10000); //进行Solr索引
                }
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                tryCount++;
            }
        }
        return false;
    }

}
