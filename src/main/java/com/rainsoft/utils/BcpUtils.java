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

    public static void transformBcpToTsv(String resourcePath, String targetpath, int captureTimeIndex) {
        File dir = FileUtils.getFile(resourcePath);
        File[] files = dir.listFiles();
        Date curDate = new Date();
        int maxFileDataSize = ConfigurationManager.getInteger("data_file_max_lines");
        int lineCount = 0;
        StringBuffer sb = new StringBuffer();
        assert files != null;
        for (File file :
                files) {
            String content = null;
            try {
                content = FileUtils.readFileToString(file, "utf-8");
            } catch (IOException e) {
                logger.info("读取文件内容失败");
                e.printStackTrace();
            }
            assert content != null;
            content = content.replace("\r\n", "")   //替换Win下的换行
                    .replace("\n", "")              //替换Linux下的换行
                    .replace("\r", "")              //替换Mac下的换行
                    .replace("\t", "");

            String[] lines = content.split(BigDataConstants.BCP_LINE_SEPARATOR);
            for (String line :
                    lines) {
                String[] fieldValues = line.split(BigDataConstants.BCP_FIELD_SEPARATOR);

                long captureTimeMinSecond;
                try {
                    captureTimeMinSecond = DateUtils.TIME_FORMAT.parse(fieldValues[captureTimeIndex]).getTime();
                } catch (Exception e) {
                    continue;
                }
                if (captureTimeMinSecond > curDate.getTime()) {
                    logger.warn("脏数据,捕获日期大于当前日期,捕获日期为：{}", fieldValues[captureTimeIndex]);
                    logger.warn("此条数据全部信息为: {}", line);
                    continue;
                }
                String uuid = UUID.randomUUID().toString().replace("-", "");
                String rowkey = captureTimeMinSecond + "_" + uuid;
                sb.append(rowkey).append("\t");

                for (int i = 0; i < fieldValues.length; i++) {
                    String fieldValue = fieldValues[i];

                    if (i < (fieldValues.length - 1)) {
                        sb.append(fieldValue).append("\t");
                    } else {
                        sb.append(fieldValue);
                    }
                }
                lineCount++;
                sb.append("\r\n");

                if (lineCount >= maxFileDataSize) {
                    //写入本地文件
                    writeStringToFile(sb, targetpath);
                    sb = new StringBuffer();
                    lineCount = 0;
                }
            }
        }
        if (sb.length() > 0) {
            //write into local file
            writeStringToFile(sb, targetpath);
        }
    }

    /**
     * 将数据写入到文件
     *
     * @param sb
     */
    private static String writeStringToFile(StringBuffer sb, String targetPath) {
        String random = RandomStringUtils.randomAlphabetic(8);
        String fileName = new Date().getTime() + "_" + random + ".tsv";
        File parentDir = new File(targetPath);
        File dataFile = new File(parentDir, fileName);
        if (!parentDir.exists() || !parentDir.isDirectory()) {
            parentDir.mkdirs();
        }
        if (dataFile.exists() || dataFile.isDirectory()) {
            dataFile.delete();
        }

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
        transformBcpToTsv("E:\\work\\RainSoft\\data\\ftp", "E:\\data\\data\\ftp", 17);
    }
}
