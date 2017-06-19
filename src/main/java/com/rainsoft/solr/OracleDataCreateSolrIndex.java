package com.rainsoft.solr;

import com.facebook.presto.sql.tree.Except;
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
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.FilenameFilter;
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

    //imchatDao
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

    public static void main(String[] args) throws IOException, SolrServerException, ParseException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        //开始日期
        Date startDate = DateUtils.parseDate(args[0], "yyyy-MM-dd");
        //结束日期
        Date endDate = DateUtils.parseDate(args[1], "yyyy-MM-dd");

        System.out.println("开始时间：" + args[0]);
        System.out.println("结束时间：" + args[1]);

        dataFileLines = Integer.valueOf(args[2]);

        createMode = args[3];

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

            ftpCreateSolrIndexByDay(captureTime);
            httpCreateSolrIndexByDay(captureTime);
            imchatCreateSolrIndexByDay(captureTime);

            //开始时间减少一天
            startDate = DateUtils.addDays(startDate, -1);
        }

        context.close();
        System.exit(0);

    }

    private static boolean imchatCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        System.out.println("imchat : 开始索引 " + captureTime + " 的数据");

        //获取数据库一天的数据
        List<RegContentImChat> datalist = imchatDao.getImchatBydate(captureTime);

        boolean flat = false;
        //一次性处理一天的数据
        if ("once".equals(createMode)) {
            flat = imchatCreateIndex(datalist, client);
        } else if ("several".equals(createMode)) {//一天的数据分批处理
            //将FTP的数据写入磁盘
            writeImchatDisk(datalist);

            File dir = FileUtils.getFile("data/imchat");

            if (dir.isDirectory() == false) {
                return false;
            }
            File[] fileList = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith("data_")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            });

            for (File file : fileList) {
                List<RegContentImChat> list = new ArrayList<>();
                List<String> ftpDataList = FileUtils.readLines(file);
                for (String line : ftpDataList) {
                    Map<String, String> map = new HashMap<>();
                    String[] kvs = line.split(kvOutSeparator);
                    for (int i = 0; i < kvs.length; i++) {
                        try {
                            map.put(kvs[i].split(kvInnerSeparator)[0], kvs[i].split(kvInnerSeparator)[1]);
                        } catch (Exception e) {
                            System.out.println(line);
                        }
                    }
                    RegContentImChat imchat = new RegContentImChat();
                    BeanUtils.populate(imchat, map);
                    list.add(imchat);
                }
                boolean importStatus = imchatCreateIndex(list, client);
                if (importStatus == true) {
                    file.delete();
                } else {
                    return false;
                }
            }

            //清空目录下的所有文件
            File[] fileRemains = dir.listFiles();
            for (File file : fileRemains) {
                file.delete();
            }
            flat = true;
        }

        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_imchat", "success");
        } else {
            recordMap.put(captureTime + "_imchat", "fail");
        }

        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        Collections.sort(newRecordList);

        String newRecords = StringUtils.join(newRecordList, "\r\n");

        FileUtils.writeStringToFile(recordFile, newRecords, false);

        System.out.println("imchat : " + captureTime + " 的数据,索引成功");
        return flat;

    }

    private static boolean httpCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        System.out.println("http : 开始索引 " + captureTime + " 的数据");

        //获取数据库一天的数据
        List<RegContentHttp> datalist = httpDao.getHttpBydate(captureTime);

        System.out.println("从数据库查询结束------>" + com.rainsoft.utils.DateUtils.TIME_FORMAT.format(new Date()));
        boolean flat = false;
        //一次性处理一天的数据
        if ("once".equals(createMode)) {
            flat = httpCreateIndex(datalist, client);
        } else if ("several".equals(createMode)) {//一天的数据分批处理
            //将FTP的数据写入磁盘
            writeHttpDisk(datalist);

            File dir = FileUtils.getFile("data/http");

            if (dir.isDirectory() == false) {
                return false;
            }
            File[] fileList = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith("data_")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            });

            for (File file : fileList) {
                List<RegContentHttp> list = new ArrayList<>();
                List<String> ftpDataList = FileUtils.readLines(file);
                for (String line : ftpDataList) {
                    Map<String, String> map = new HashMap<>();
                    String[] kvs = line.split(kvOutSeparator);
                    for (int i = 0; i < kvs.length; i++) {
                        map.put(kvs[i].split(kvInnerSeparator)[0], kvs[i].split(kvInnerSeparator)[1]);
                    }
                    RegContentHttp http = new RegContentHttp();
                    BeanUtils.populate(http, map);
                    list.add(http);
                }
                boolean importStatus = httpCreateIndex(list, client);
                if (importStatus == true) {
                    file.delete();
                } else {
                    return false;
                }
            }

            //清空目录下的所有文件
            File[] fileRemains = dir.listFiles();
            for (File file : fileRemains) {
                file.delete();
            }
            flat = true;
        }

        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_http", "success");
        } else {
            recordMap.put(captureTime + "_http", "fail");
        }

        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        Collections.sort(newRecordList);

        String newRecords = StringUtils.join(newRecordList, "\r\n");

        FileUtils.writeStringToFile(recordFile, newRecords, false);

        System.out.println("http : " + captureTime + " 的数据,索引成功");
        return flat;

    }


    public static boolean imchatCreateIndex(List<RegContentImChat> datalist, SolrClient client) throws IOException, SolrServerException {
        //缓冲数据
        List<SolrInputDocument> cacheList = new ArrayList<>();

        int dataCount = 0;
        //进行Solr索引
        for (RegContentImChat imChat : datalist) {
            //创建SolrImputDocument实体
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

            //索引实体添加缓冲区
            cacheList.add(doc);
            dataCount++;
        }
        System.out.println("当前索引的数据量 = " + dataCount);

        return submitSolr(cacheList, client);
    }

    public static boolean httpCreateIndex(List<RegContentHttp> datalist, SolrClient client) throws IOException, SolrServerException {
        //缓冲数据
        List<SolrInputDocument> cacheList = new ArrayList<>();

        int dataCount = 0;
        //进行Solr索引
        for (RegContentHttp http : datalist) {
            //创建SolrImputDocument实体
            SolrInputDocument doc = new SolrInputDocument();

            String uuid = UUID.randomUUID().toString().replace("-", "");
            http.setId(uuid);

            doc.addField("docType", "网页");

            //数据实体属性集合
            Field[] fields = RegContentHttp.class.getFields();

            //生成Solr导入实体
            for (Field field : fields) {
                String fieldName = field.getName();
                doc.addField(fieldName.toUpperCase(), ReflectUtils.getFieldValueByName(fieldName, http));
            }

            //索引实体添加缓冲区
            cacheList.add(doc);
            dataCount++;
        }
        System.out.println("当前数据量 = " + dataCount);

        return submitSolr(cacheList, client);
    }

    private static boolean ftpCreateSolrIndexByDay(String captureTime) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        System.out.println("FTP : 开始索引 " + captureTime + " 的数据");

        //获取数据库一天的数据
        List<RegContentFtp> datalist = ftpDao.getFtpBydate(captureTime);

        boolean flat = false;
        //一次性处理一天的数据
        if ("once".equals(createMode)) {
            flat = ftpcreateIndex(datalist, client);
        } else if ("several".equals(createMode)) {//一天的数据分批处理
            //将FTP的数据写入磁盘
            writeFtpDisk(datalist);

            File dir = FileUtils.getFile("data/ftp");

            if (dir.isDirectory() == false) {
                return false;
            }
            File[] fileList = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith("data_")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            });

            for (File file : fileList) {
                List<RegContentFtp> list = new ArrayList<>();
                List<String> ftpDataList = FileUtils.readLines(file);
                for (String line : ftpDataList) {
                    Map<String, String> map = new HashMap<>();
                    String[] kvs = line.split(kvOutSeparator);
                    for (int i = 0; i < kvs.length; i++) {
                        map.put(kvs[i].split(kvInnerSeparator)[0], kvs[i].split(kvInnerSeparator)[1]);
                    }
                    RegContentFtp ftp = new RegContentFtp();
                    BeanUtils.populate(ftp, map);
                    list.add(ftp);
                }
                boolean importStatus = ftpcreateIndex(list, client);
                if (importStatus == true) {
                    file.delete();
                } else {
                    return false;
                }
            }

            //清空目录下的所有文件
            File[] fileRemains = dir.listFiles();
            for (File file : fileRemains) {
                file.delete();
            }
            flat = true;
        }

        //数据索引结果成功或者失败写入记录文件,
        if (flat) {
            recordMap.put(captureTime + "_ftp", "success");
        } else {
            recordMap.put(captureTime + "_ftp", "fail");
        }

        List<String> newRecordList = recordMap.entrySet().stream().map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList());

        Collections.sort(newRecordList);

        String newRecords = StringUtils.join(newRecordList, "\r\n");

        FileUtils.writeStringToFile(recordFile, newRecords, false);

        System.out.println("FTP : " + captureTime + " 的数据,索引成功");
        return flat;
    }


    public static void writeFtpDisk(List<RegContentFtp> datalist) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        int lineCount = 0;
        int writeCount = 0;
        int fileIndex = 0;

        File dataFile = new File("data/ftp/data_" + fileIndex);
        File parent = dataFile.getParentFile();
        if (parent.exists() == false) {
            parent.mkdirs();
        }
        if (dataFile.exists() == false) {
            System.out.println("生成第 " + (fileIndex + 1) + " 个数据文件");
            dataFile.createNewFile();
        }

        PropertyUtilsBean bean = new PropertyUtilsBean();
        StringBuilder sb = new StringBuilder();
        for (RegContentFtp ftp : datalist) {
            PropertyDescriptor[] descriptor = bean.getPropertyDescriptors(ftp);
            for (int i = 0; i < descriptor.length; i++) {
                String name = descriptor[i].getName();
                if ("class".equals(name) == false) {
                    if (descriptor.length - i > 1) {
                        sb.append(name + "|=|" + bean.getNestedProperty(ftp, name) + "|;|");
                    } else {
                        sb.append(name + "|=|" + bean.getNestedProperty(ftp, name) + "\r\n");
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
        datalist.clear();
    }

    public static void writeHttpDisk(List<RegContentHttp> datalist) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        int lineCount = 0;
        int writeCount = 0;
        int fileIndex = 0;

        File dataFile = new File("data/http/data_" + fileIndex);
        File parent = dataFile.getParentFile();
        if (parent.exists() == false) {
            parent.mkdirs();
        }
        if (dataFile.exists() == false) {
            dataFile.createNewFile();
            System.out.println("生成第 1 个数据文件");
        }

        PropertyUtilsBean bean = new PropertyUtilsBean();
        StringBuilder sb = new StringBuilder();
        for (RegContentHttp http : datalist) {
            PropertyDescriptor[] descriptor = bean.getPropertyDescriptors(http);
            for (int i = 0; i < descriptor.length; i++) {
                String name = descriptor[i].getName();
                if ("class".equals(name) == false) {
                    if (descriptor.length - i > 1) {
                        sb.append(name + "|=|" + bean.getNestedProperty(http, name) + "|;|");
                    } else {
                        sb.append(name + "|=|" + bean.getNestedProperty(http, name) + "\r\n");
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

        datalist.clear();
    }

    public static void writeImchatDisk(List<RegContentImChat> datalist) throws IOException, SolrServerException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        int lineCount = 0;
        int writeCount = 0;
        int fileIndex = 0;

        File dataFile = new File("data/imchat/data_" + fileIndex);
        File parent = dataFile.getParentFile();
        if (parent.exists() == false) {
            parent.mkdirs();
        }
        if (dataFile.exists() == false) {
            dataFile.createNewFile();
            System.out.println("生成第 1 个数据文件");
        }

        PropertyUtilsBean bean = new PropertyUtilsBean();
        StringBuilder sb = new StringBuilder();
        for (RegContentImChat imChat : datalist) {
            PropertyDescriptor[] descriptor = bean.getPropertyDescriptors(imChat);
            for (int i = 0; i < descriptor.length; i++) {
                String name = descriptor[i].getName();
                if ("class".equals(name) == false) {
                    String value = (String) bean.getNestedProperty(imChat, name);
                    if (value != null) {
                        value = value.replace("\r", "").replace("\n", "");
                    }
                    if (descriptor.length - i > 1) {
                        sb.append(name + "|=|" + value + "|;|");
                    } else {
                        sb.append(name + "|=|" + value + "\r\n");
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

    public static boolean ftpcreateIndex(List<RegContentFtp> datalist, SolrClient client) throws IOException, SolrServerException {
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

        datalist.clear();
        System.out.println("当前索引数据量 = " + dataCount);

        return submitSolr(cacheList, client);
    }

    /**
     * 提交导入Solr索引
     *
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
