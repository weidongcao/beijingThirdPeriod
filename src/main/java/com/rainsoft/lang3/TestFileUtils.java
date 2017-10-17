package com.rainsoft.lang3;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Created by Administrator on 2017-10-17.
 */
public class TestFileUtils {
    //系统分隔符
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static void main(String[] args) {
        String record = "reg_content_bbs-last_export_time\t2017-10-17 00:00:00";
        File recordFile;
        //导入记录
        String importRecordFile = "createIndexRecord/index-record.txt";
        //转换文件分隔符,使在Window和Linux下都可以使用
        String convertImportRecordFile = importRecordFile.replace("/", FILE_SEPARATOR).replace("\\", FILE_SEPARATOR);
        //创建导入记录文件
        recordFile = FileUtils.getFile(convertImportRecordFile);
        File parentFile = recordFile.getParentFile();
    }
}
