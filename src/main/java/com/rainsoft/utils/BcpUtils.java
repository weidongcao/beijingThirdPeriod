package com.rainsoft.utils;

import com.rainsoft.BigDataConstants;
import com.rainsoft.conf.ConfigurationManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * 处理BCP文件工具类
 * Created by CaoWeiDong on 2017-08-09.
 */
public class BcpUtils {
    public static final Logger logger = LoggerFactory.getLogger(BcpUtils.class);

    /**
     * 替换BCP数据文件中非正常的回车换行
     *
     * @param path
     */
    public void replaceFileRN(String path) {
        File dir = FileUtils.getFile(path);

        File[] files = dir.listFiles();
        assert files != null;
        for (File file : files) {
            try {
                String content = FileUtils.readFileToString(file, "utf-8");
                content = content.replace("\r\n", "")   //替换Win下的换行
                        .replace("\n", "")              //替换Linux下的换行
                        .replace("\r", "");              //替换Mac下的换行
                content = content.replaceAll(BigDataConstants.BCP_LINE_SEPARATOR, BigDataConstants.BCP_LINE_SEPARATOR + "\r\n");
                FileUtils.writeStringToFile(file, content, "utf-8", false);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        logger.info("回车换行符替换完成,替换路径： {}", path);
    }

    /**
     * 将BCP文件转为Tsv文件
     * @param resourcePath  BCP文件所在目录
     * @param targetPath    转换成Tsv文件后存储的目录
     * @param captureTimeIndex  捕获时间是是每几个字段(从0开始)
     */
    public static void transformBcpToTsv(String resourcePath, String targetPath, String taskType, int captureTimeIndex) {
        //BCP文件所在目录
        File dir = FileUtils.getFile(resourcePath);
        //BCP文件列表
        File[] files = dir.listFiles();
        //当前时间
        Date curDate = new Date();
        //转成TSV文件后最大行数
        int maxFileDataSize = ConfigurationManager.getInteger("data.file.max.lines");
        //统计BCP文件行数
        int lineCount = 0;
        //写入TSV文件前的缓存
        StringBuffer sb = new StringBuffer();

        //遍历BCP文件将转换后的内容写入TSV
        assert files != null;
        for (File file : files) {
            //将一个BCP文件读出为字符串
            String content = null;
            try {
                content = FileUtils.readFileToString(file, "utf-8");
            } catch (IOException e) {
                logger.info("读取文件内容失败");
                e.printStackTrace();
            }
            //替换BCP文件中所有的换行
            assert content != null;
            content = content.replace("\r\n", "")   //替换Win下的换行
                    .replace("\n", "")              //替换Linux下的换行
                    .replace("\r", "")              //替换Mac下的换行
                    .replace("\t", "");

            //将BCP文件内容按BCP文件列分隔符(|$|)切分为数组
            String[] lines = content.split(BigDataConstants.BCP_LINE_SEPARATOR);

            //按列将BCP格式转为TSV格式
            for (String line : lines) {
                //将BCP文件的一列数据按BCP的字段分隔符(|#|)切分成多列的值
                String[] fieldValues = line.split(BigDataConstants.BCP_FIELD_SEPARATOR);

                //捕获时间的毫秒，HBase按毫秒将同一时间捕获的数据聚焦到一起
                long captureTimeMinSecond;
                try {
                    captureTimeMinSecond = DateUtils.TIME_FORMAT.parse(fieldValues[captureTimeIndex]).getTime();
                } catch (Exception e) {
                    continue;
                }

                //捕获时间的毫秒+UUID作为数据的ID(HBase的rowKey,Solr的SID, Oracle的ID)
                String uuid = UUID.randomUUID().toString().replace("-", "");
                String rowKey = captureTimeMinSecond + "_" + uuid;
                sb.append(rowKey).append("\t");

                //写入StringBuffer缓存
                for (int i = 0; i < fieldValues.length; i++) {
                    String fieldValue = fieldValues[i];

                    if (i < (fieldValues.length - 1)) {
                        sb.append(fieldValue).append("\t");
                    } else {
                        sb.append(fieldValue);
                    }
                }
                //行数加1
                lineCount++;
                //换行
                sb.append("\r\n");

                //达到最大行数写入TSV文件
                if (lineCount >= maxFileDataSize) {
                    //写入本地文件
                    writeStringToFile(sb, targetPath, taskType);
                    sb = new StringBuffer();
                    lineCount = 0;
                }
            }
        }
        //写入TSV文件
        if (sb.length() > 0) {
            //write into local file
            writeStringToFile(sb, targetPath, taskType);
        }
    }

    /**
     * 将转换后的TSV数据写入文件
     * 转换后的TSV文件命名：${数据类型(ftp/http/im_chat)}_${日期的毫秒数}_${8位随机字符串}.tsv
     *
     * @param sb    TSV数据内容
     * @param targetPath    TSV文件路径
     * @return
     */
    private static String writeStringToFile(StringBuffer sb, String targetPath, String taskType) {
        //生成8位随机数
        String random = RandomStringUtils.randomAlphabetic(8);
        //根据时间的毫秒及8位随机数合为TSV文件 的名字
        String fileName = taskType + "_data_" + new Date().getTime() + "_" + random + ".tsv";

        //判断目录及文件是否存在
        File parentDir = new File(targetPath);
        File dataFile = new File(parentDir, fileName);
        if (!parentDir.exists() || !parentDir.isDirectory()) {
            parentDir.mkdirs();
        }
        if (dataFile.exists() || dataFile.isDirectory()) {
            dataFile.delete();
        }

        //写入文件
        try {
            dataFile.createNewFile();
            FileUtils.writeStringToFile(dataFile, sb.toString(), "utf-8", false);
            logger.info("BCP生成Tsv文件成功");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("生产数据文件失败....");
            System.exit(-1);
        }
        return fileName;
    }

    public static void main(String[] args) {
        transformBcpToTsv("E:\\work\\RainSoft\\data\\ftp", "E:\\data\\data\\ftp", "ftp", 17);
    }
}
