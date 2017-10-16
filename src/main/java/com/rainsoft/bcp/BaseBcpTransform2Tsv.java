package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * 处理BCP文件工具类
 * Created by CaoWeiDong on 2017-08-09.
 */
public abstract class BaseBcpTransform2Tsv {
    public static final Logger logger = LoggerFactory.getLogger(BaseBcpTransform2Tsv.class);

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
     */
    public void transformBcpToTsv(String resourcePath, String targetPath) {

    }

    /**
     * 将转换后的TSV数据写入文件
     * 转换后的TSV文件命名：${数据类型(ftp/http/im_chat)}_${日期的毫秒数}_${8位随机字符串}.tsv
     *
     * @param sb    TSV数据内容
     * @param targetPath    TSV文件路径
     * @return
     */
    public static String writeStringToFile(StringBuffer sb, String targetPath, String taskType) {
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

    /**
     * 检测数据关键字段是否有为空，
     * @param fieldValues 要检测的数组
     * @param filterIndex 要检测的下标数组
     * @return 是否过滤(false: 不过滤, true: 过滤)
     */
    public static boolean validColumns(String[] fieldValues, int[] filterIndex) {
        boolean ifFilter = false;
        for (int index :
                filterIndex) {
            if (StringUtils.isBlank(fieldValues[index])) {
                ifFilter = true;
                break;
            }
        }
        return ifFilter;
    }
}
