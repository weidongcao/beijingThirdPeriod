package com.rainsoft.solr;

import com.rainsoft.dao.FtpDao;
import com.rainsoft.dao.HttpDao;
import com.rainsoft.dao.ImchatDao;
import com.rainsoft.domain.RegContentFtp;
import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.domain.RegContentImChat;
import com.rainsoft.utils.ReflectUtils;
import com.rainsoft.utils.SolrUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
    private static int dataFileLines = 1000000;
    //一次写入文件的数据量
    private static int writeSize = 100000;
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    //创建Spring Context
    private static AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring-module.xml");

    //ftpDao
    private static FtpDao ftpDao = (FtpDao) context.getBean("ftpDao");

    //httpDao
    private static HttpDao httpDao = (HttpDao) context.getBean("httpDao");

    //imChatDao
    private static ImchatDao imchatDao = (ImchatDao) context.getBean("imchatDao");

    //创建Solr客户端
    private static CloudSolrClient client = SolrUtil.getSolrClient("yisou");

    //导入记录文件
    private static File recordFile;

    //导入记录
    private static Map<String, String> recordMap = new HashMap<>();

    //创建模式：一次创建，多次创建
    private static String createMode;

    //字段间分隔符
    private static String kvOutSeparator = "\\|;\\|";

    private static String kvInnerSeparator = "\\|=\\|";

    private static String FTP = "ftp";
    private static String HTTP = "http";
    private static String IMCHAT = "imchat";
    private static String SUCCESS_STATUS = "success";
    private static String FAIL_STATUS = "fail";

    static {
        //导入记录
        String importRecordFile = "createIndexRecord/index-record.txt";
        //转换文件分隔符,使在Window和Linux下都可以使用
        String convertImportRecordFile = importRecordFile.replace("/", FILE_SEPARATOR).replace("\\", FILE_SEPARATOR);
        //创建导入记录文件
        recordFile = FileUtils.getFile(convertImportRecordFile);
        File parentFile = recordFile.getParentFile();

        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }

        if (!recordFile.exists()) {
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
        assert recordsList != null;
        for (String record : recordsList) {
            String[] kv = record.split("\t");
            recordMap.put(kv[0], kv[1]);
        }
    }

    public static void main(String[] args) throws IOException, SolrServerException, ParseException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        //开始日期
        Date startDate = DateUtils.parseDate(args[0], "yyyy-MM-dd");
        //结束日期
        Date endDate = DateUtils.parseDate(args[1], "yyyy-MM-dd");

        System.out.println("参数 --> 开始日期：" + args[0]);
        System.out.println("参数 --> 结束日期：" + args[1]);

        createMode = args[2];
        if (args.length == 4) {
            dataFileLines = Integer.valueOf(args[3]);
        }

        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 程序启动");
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

            /*
             * 导入FTP的数据，如果还没有导入或者导入失败，且前一次任务导入成功则执行导入程序
             * 如果已经导入成功过了，则不再导入
             *
             */
            if (!SUCCESS_STATUS.equals(recordMap.get(ftpRecord)) && importResultStatus) {
                importResultStatus = ftpCreateSolrIndexByDay(captureTime);
            } else if (SUCCESS_STATUS.equals(recordMap.get(ftpRecord))) {
                System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " : " + captureTime + " : " + FTP + " has already imported");
            }

            /*
             * 导入FTP的数据，如果还没有导入或者导入失败，且前一次任务导入成功则执行导入程序
             * 如果已经导入成功过了，则不再导入
             */
            String httpRecord = captureTime + "_" + HTTP;
            if (!SUCCESS_STATUS.equals(recordMap.get(httpRecord)) && importResultStatus) {
                importResultStatus = httpCreateSolrIndexByDay(captureTime);
            } else if (SUCCESS_STATUS.equals(recordMap.get(httpRecord))) {
                System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " : " + captureTime + " : " + HTTP + " has already imported");
            }

            /*
             * 导入FTP的数据，如果还没有导入或者导入失败，且前一次任务导入成功则执行导入程序
             * 如果已经导入成功过了，则不再导入
             */
            String imchatRecord = captureTime + "_" + IMCHAT;
            if (!SUCCESS_STATUS.equals(recordMap.get(imchatRecord)) && importResultStatus) {
                importResultStatus = imChatCreateSolrIndexByDay(captureTime);
            } else if (SUCCESS_STATUS.equals(recordMap.get(imchatRecord))) {
                System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " : " + captureTime + " : " + IMCHAT + " has already imported");
            }

            //开始时间减少一天
            startDate = DateUtils.addDays(startDate, -1);
            if (importResultStatus) {
                System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " " + captureTime + " 数据索引完毕");
            } else {
                System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " " + captureTime + " 数据导入异常, 执行结束");
            }

            long endDayTime = new Date().getTime();
            long runTime = (endDayTime - startDayTime) / 1000;
            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 索引" + captureTime + "天的数据,程序执行时间: " + runTime / 60 + "分钟" + runTime % 60 + "秒");
        }
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 启动程序执行完毕");
        long endTime = new Date().getTime();

        long totalRunTime = (endTime - startTime) / 1000;
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 程序总共执行时间: " + totalRunTime / 60 + "分钟" + totalRunTime % 60 + "秒");
        context.close();
        System.exit(0);

    }

    private static boolean imChatCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " imChat : 开始索引 " + captureTime + " 的数据");

        boolean flat = false;
        //一次性处理一天的数据

        //获取数据库一天的数据
        List<RegContentImChat> dataList = imchatDao.getImchatBydate(captureTime);

        if ("once".equals(createMode)) {
            flat = imChatCreateIndex(dataList, client);
        } else if ("several".equals(createMode)) {//一天的数据分批处理
            //将FTP的数据写入磁盘;
            writeImChatDisk(dataList);

            File dir = FileUtils.getFile("data/imChat");

            if (!dir.isDirectory()) {
                return false;
            }
            File[] fileList = dir.listFiles((dir1, name) -> name.startsWith("data_"));

            assert fileList != null;
            for (File file : fileList) {
                List<RegContentImChat> list = new ArrayList<>();
                List<String> ftpDataList = FileUtils.readLines(file);
                for (String line : ftpDataList) {
                    Map<String, String> map = new HashMap<>();
                    String[] kvs = line.split(kvOutSeparator);
                    for (String kv : kvs) {
                        try {
                            map.put(kv.split(kvInnerSeparator)[0], kv.split(kvInnerSeparator)[1]);
                        } catch (Exception e) {
                            System.out.println(line);
                        }
                    }
                    RegContentImChat imChat = new RegContentImChat();
                    BeanUtils.populate(imChat, map);
                    list.add(imChat);
                }
                boolean importStatus = imChatCreateIndex(list, client);
                if (importStatus) {
                    file.delete();
                } else {
                    return false;
                }
            }

            //清空目录下的所有文件
            File[] fileRemains = dir.listFiles();
            assert fileRemains != null;
            for (File file : fileRemains) {
                file.delete();
            }
            flat = true;
        }

        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_" + IMCHAT, SUCCESS_STATUS);
        } else {
            recordMap.put(captureTime + "_" + IMCHAT, FAIL_STATUS);
            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 当天数据导入失败");
        }

        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        Collections.sort(newRecordList);

        String newRecords = StringUtils.join(newRecordList, "\r\n");

        FileUtils.writeStringToFile(recordFile, newRecords, false);

        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " imchat : " + captureTime + " 的数据,索引完成");
        return flat;

    }

    private static boolean httpCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " http : 开始索引 " + captureTime + " 的数据");
        boolean flat = false;

        float f = 0f;
        while (f < 1) {
            float startPercent = f;
            float endPercent;
            if (f + 0.3 <= 1) {
                endPercent = (float) (f + 0.3);
            } else {
                endPercent = 1f;
            }
            //获取数据库一天的数据
            List<RegContentHttp> dataList = httpDao.getHttpBydate(captureTime, startPercent, endPercent);

            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 从数据库查询结束------>");

            //一次性处理一天的数据
            if ("once".equals(createMode)) {
                flat = httpCreateIndex(dataList, client);
            } else if ("several".equals(createMode)) {//一天的数据分批处理
                //将FTP的数据写入磁盘
                writeHttpDisk(dataList);

                File dir = FileUtils.getFile("data/http");

                if (!dir.isDirectory()) {
                    return false;
                }
                File[] fileList = dir.listFiles((dir1, name) -> name.startsWith("data_"));

                assert fileList != null;
                for (File file : fileList) {
                    List<RegContentHttp> list = new ArrayList<>();
                    List<String> ftpDataList = FileUtils.readLines(file);
                    for (String line : ftpDataList) {
                        Map<String, String> map = new HashMap<>();
                        String[] kvs = line.split(kvOutSeparator);
                        for (String kv : kvs) {
                            map.put(kv.split(kvInnerSeparator)[0], kv.split(kvInnerSeparator)[1]);
                        }
                        RegContentHttp http = new RegContentHttp();
                        BeanUtils.populate(http, map);
                        list.add(http);
                    }
                    boolean importStatus = httpCreateIndex(list, client);
                    if (importStatus) {
                        file.delete();
                    } else {
                        return false;
                    }
                }

                //清空目录下的所有文件
                File[] fileRemains = dir.listFiles();
                assert fileRemains != null;
                for (File file : fileRemains) {
                    file.delete();
                }
                flat = true;
            }

            f += -0.3;
        }
        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_" + HTTP, SUCCESS_STATUS);
        } else {
            recordMap.put(captureTime + "_" + HTTP, FAIL_STATUS);
            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 当天数据导入失败");

        }

        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        Collections.sort(newRecordList);

        String newRecords = StringUtils.join(newRecordList, "\r\n");

        FileUtils.writeStringToFile(recordFile, newRecords, false);

        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " http : " + captureTime + " 的数据,索引完成");
        return flat;

    }


    private static boolean imChatCreateIndex(List<RegContentImChat> dataList, SolrClient client) throws IOException, SolrServerException {
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 当前要索引的数据量 = " + dataList.size());
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

                doc.addField("docType", "聊天");

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
                    System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 聊天采集时间转换失败,采集时间为： " + imChat.getCapture_time());
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

            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 第" + submitCount + "次索引10万条数据成功;剩余未索引的数据: " + dataList.size() + "条");
        }

        long endIndexTime = new Date().getTime();
        //计算索引一天的数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " IMCHAT索引一天的数据执行时间: " + indexRunTime / 60 + "分钟" + indexRunTime % 60 + "秒");

        return flat;
    }

    private static boolean httpCreateIndex(List<RegContentHttp> dataList, SolrClient client) throws IOException, SolrServerException {
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 当前要索引的数据量 = " + dataList.size());
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
                http.setId(uuid);

                doc.addField("docType", "网络");

                //数据实体属性集合
                Field[] fields = RegContentHttp.class.getFields();

                //生成Solr导入实体
                for (Field field : fields) {
                    String fieldName = field.getName();
                    doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, http));
                }
                //导入时间
                doc.addField("IMPORT_TIME", com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()));
                try {
                    doc.addField("capture_time", com.rainsoft.utils.DateUtils.TIME_FORMAT.parse(http.getCapture_time().split("\\.")[0]).getTime());
                } catch (ParseException e) {
                    System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " HTTP采集时间转换失败,采集时间为： " + http.getCapture_time());
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

            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 第" + submitCount + "次索引10万条数据成功;剩余未索引的数据: " + dataList.size() + "条");
        }

        long endIndexTime = new Date().getTime();
        //计算索引一天的数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " HTTP索引一天的数据执行时间: " + indexRunTime / 60 + "分钟" + indexRunTime % 60 + "秒");

        return flat;
    }

    private static boolean ftpCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ParseException {
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + "FTP : 开始索引 " + captureTime + " 的数据");

        //获取数据库一天的数据
        List<RegContentFtp> dataList = ftpDao.getFtpBydate(captureTime);

        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 从数据库查询数据结束");
        boolean flat = false;
        //一次性处理一天的数据
        if ("once".equals(createMode)) {
            flat = ftpCreateIndex(dataList, client);
        } else if ("several".equals(createMode)) {//一天的数据分批处理
            //将FTP的数据写入磁盘
            writeFtpDisk(dataList);

            File dir = FileUtils.getFile("data/ftp");

            if (!dir.isDirectory()) {
                return false;
            }
            File[] fileList = dir.listFiles((dir1, name) -> name.startsWith("data_"));

            assert fileList != null;
            for (File file : fileList) {
                List<RegContentFtp> list = new ArrayList<>();
                List<String> ftpDataList = FileUtils.readLines(file);
                for (String line : ftpDataList) {
                    Map<String, String> map = new HashMap<>();
                    String[] kvs = line.split(kvOutSeparator);
                    for (String kv : kvs) {
                        map.put(kv.split(kvInnerSeparator)[0], kv.split(kvInnerSeparator)[1]);
                    }
                    RegContentFtp ftp = new RegContentFtp();
                    BeanUtils.populate(ftp, map);
                    list.add(ftp);
                }
                boolean importStatus = ftpCreateIndex(list, client);
                if (importStatus) {
                    file.delete();
                } else {
                    return false;
                }
            }

            //清空目录下的所有文件
            File[] fileRemains = dir.listFiles();
            assert fileRemains != null;
            for (File file : fileRemains) {
                file.delete();
            }
            flat = true;
        }

        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_" + FTP, SUCCESS_STATUS);
        } else {
            recordMap.put(captureTime + "_" + FTP, FAIL_STATUS);
            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 当天数据导入失败");
        }

        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        Collections.sort(newRecordList);

        String newRecords = StringUtils.join(newRecordList, "\r\n");

        FileUtils.writeStringToFile(recordFile, newRecords, false);

        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " FTP : " + captureTime + " 的数据,索引完成");
        return flat;
    }


    public static void writeFtpDisk(List<RegContentFtp> dataList) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        int lineCount = 0;
        int writeCount = 0;
        int fileIndex = 0;

        File dataFile = new File("data/ftp/data_" + fileIndex);
        File parent = dataFile.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        if (!dataFile.exists()) {
            System.out.println("生成第 " + (fileIndex + 1) + " 个数据文件");
            dataFile.createNewFile();
        }

        PropertyUtilsBean bean = new PropertyUtilsBean();
        StringBuilder sb = new StringBuilder();
        for (RegContentFtp ftp : dataList) {
            PropertyDescriptor[] descriptor = bean.getPropertyDescriptors(ftp);
            for (int i = 0; i < descriptor.length; i++) {
                String name = descriptor[i].getName();
                if (!"class".equals(name)) {
                    if (descriptor.length - i > 1) {
                        sb.append(name).append("|=|").append(bean.getNestedProperty(ftp, name)).append("|;|");
                    } else {
                        sb.append(name).append("|=|").append(bean.getNestedProperty(ftp, name)).append("\r\n");
                    }
                }
            }

            lineCount++;
            writeCount++;

            if (lineCount >= dataFileLines) {
                fileIndex++;
                lineCount = 0;

                System.out.println("生成第 " + (fileIndex + 1) + " 个数据文件");
                dataFile = new File("data/ftp/data_" + fileIndex);
            }

            if (writeCount >= writeSize) {
                FileUtils.writeStringToFile(dataFile, sb.toString(), true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.writeStringToFile(dataFile, sb.toString(), true);
        sb.delete(0, sb.length());
        dataList.clear();
    }

    private static void writeHttpDisk(List<RegContentHttp> dataList) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        int lineCount = 0;
        int writeCount = 0;
        int fileIndex = 0;

        File dataFile = new File("data/http/data_" + fileIndex);
        File parent = dataFile.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        if (!dataFile.exists()) {
            dataFile.createNewFile();
            System.out.println("生成第 1 个数据文件");
        }

        PropertyUtilsBean bean = new PropertyUtilsBean();
        StringBuilder sb = new StringBuilder();
        for (RegContentHttp http : dataList) {
            PropertyDescriptor[] descriptor = bean.getPropertyDescriptors(http);
            for (int i = 0; i < descriptor.length; i++) {
                String name = descriptor[i].getName();
                if (!"class".equals(name)) {
                    if (descriptor.length - i > 1) {
                        sb.append(name).append("|=|").append(bean.getNestedProperty(http, name)).append("|;|");
                    } else {
                        sb.append(name).append("|=|").append(bean.getNestedProperty(http, name)).append("\r\n");
                    }
                }
            }

            lineCount++;
            writeCount++;

            if (lineCount > dataFileLines) {
                fileIndex++;

                lineCount = 0;

                System.out.println("生成第 " + (fileIndex + 1) + " 个数据文件");
                dataFile = new File("data/http/data_" + fileIndex);
            }
            if (writeCount >= writeSize) {
                FileUtils.writeStringToFile(dataFile, sb.toString(), true);
                sb.delete(0, sb.length());

            }
        }
        FileUtils.writeStringToFile(dataFile, sb.toString(), true);
        sb.delete(0, sb.length());

        dataList.clear();
    }

    private static void writeImChatDisk(List<RegContentImChat> datalist) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        int lineCount = 0;
        int writeCount = 0;
        int fileIndex = 0;

        File dataFile = new File("data/imChat/data_" + fileIndex);
        File parent = dataFile.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        if (!dataFile.exists()) {
            dataFile.createNewFile();
            System.out.println("生成第 1 个数据文件");
        }

        PropertyUtilsBean bean = new PropertyUtilsBean();
        StringBuilder sb = new StringBuilder();
        for (RegContentImChat imChat : datalist) {
            PropertyDescriptor[] descriptor = bean.getPropertyDescriptors(imChat);
            for (int i = 0; i < descriptor.length; i++) {
                String name = descriptor[i].getName();
                if (!"class".equals(name)) {
                    String value = (String) bean.getNestedProperty(imChat, name);
                    if (value != null) {
                        value = value.replace("\r", "").replace("\n", "");
                    }
                    if (descriptor.length - i > 1) {
                        sb.append(name).append("|=|").append(value).append("|;|");
                    } else {
                        sb.append(name).append("|=|").append(value).append("\r\n");
                    }
                }
            }

            lineCount++;
            writeCount++;

            if (lineCount >= dataFileLines) {
                fileIndex++;
                lineCount = 0;

                System.out.println("生成第 " + (fileIndex + 1) + " 个数据文件");
                dataFile = new File("data/imchat/data_" + fileIndex);
            }

            if (writeCount >= writeSize) {
                FileUtils.writeStringToFile(dataFile, sb.toString(), true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.writeStringToFile(dataFile, sb.toString(), true);
        sb.delete(0, sb.length());

        datalist.clear();
    }

    private static boolean ftpCreateIndex(List<RegContentFtp> dataList, SolrClient client) throws IOException, SolrServerException {
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 当前要索引的数据量 = " + dataList.size());
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
            List<RegContentFtp> sublist;
            if (writeSize < dataList.size()) {
                sublist = dataList.subList(0, writeSize);
            } else {
                sublist = dataList;
            }

            /*
             * 将Oracle查询出来的数据封装为Solr的导入实体
             */
            for (RegContentFtp ftp : sublist) {
                //创建SolrInputDocument实体
                SolrInputDocument doc = new SolrInputDocument();

                String uuid = UUID.randomUUID().toString().replace("-", "");
                ftp.setId(uuid);

                doc.addField("docType", "文件");

                //数据实体属性集合
                Field[] fields = RegContentFtp.class.getFields();

                //生成Solr导入实体
                for (Field field : fields) {
                    String fieldName = field.getName();
                    doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, ftp));
                }
                //导入时间
                doc.addField("IMPORT_TIME", com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()));
                try {
                    doc.addField("capture_time", com.rainsoft.utils.DateUtils.TIME_FORMAT.parse(ftp.getCapture_time().split("\\.")[0]).getTime());
                } catch (Exception e) {
                    System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " FTP采集时间转换失败,采集时间为： " + ftp.getCapture_time());
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

            System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " 第" + submitCount + "次索引10万条数据成功;剩余未索引的数据: " + dataList.size() + "条");
        }

        long endIndexTime = new Date().getTime();
        //计算索引一天的数据执行的时间（秒）
        long indexRunTime = (endIndexTime - startIndexTime) / 1000;
        System.out.println(com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()) + " FTP索引一天的数据执行时间: " + indexRunTime / 60 + "分钟" + indexRunTime % 60 + "秒");

        return flat;
    }

    /**
     * 提交导入Solr索引
     *
     * @param cacheList 要导入Solr的索引
     * @param client    SolrClient
     * @return 提交状态
     */
    private static boolean submitSolr(List<SolrInputDocument> cacheList, SolrClient client) {
        /*
         * 异常捕获
         * 如果失败尝试3次
         */
        int tryCount = 0;
        boolean flat = false;
        while (tryCount < 3) {
            try {
                if (!cacheList.isEmpty()) {
                    client.add(cacheList, 1000);
                }
                flat = true;
            } catch (Exception e) {
                e.printStackTrace();
                tryCount++;
                flat = false;
            }
        }
        return flat;
    }

}
