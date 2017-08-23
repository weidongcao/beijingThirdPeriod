package com.rainsoft.oracle;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.jdbc.JDBCHelper;
import com.rainsoft.utils.DateUtils;
import com.rainsoft.utils.JdbcUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.ParseException;
import java.util.Date;

/**
 * Oracle 数据导出
 * Created by CaoWeiDong on 2017-08-05.
 */
public class LoadOracleData {
    private static final Logger logger = LoggerFactory.getLogger(LoadOracleData.class);

//    private static int maxFileDataSize = ConfigurationManager.getInteger("max_oracle_data_file_lines");
//    private static String path = ConfigurationManager.getProperty("load_oracle_data_local_dir");

    public static void main(String[] args) throws ParseException, IOException {
        String tableName = args[0];
        String startTime = args[1];
        String endTime = args[2];

        String templateSql = "select * from ${tableName} where capture_time >= to_date('${startTime}' ,'YYYY-MM-DD HH24:MI:SS') and capture_time < to_date('${endTime}' ,'YYYY-MM-DD HH24:MI:SS')";
        String sql = templateSql.replace("${tableName}", tableName);
        sql = sql.replace("${startTime}", startTime);
        sql = sql.replace("${endTime}", endTime);

        int maxFileDataSize = ConfigurationManager.getInteger("data_file_max_lines");

        JDBCHelper helper = JDBCHelper.getInstance();

        logger.info("开始执行sql: {}", sql);
        logger.info("数据文件本地生成目录: {}", ConfigurationManager.getProperty("load_data_workspace") + File.separator + "data" + File.separator + tableName);
        logger.info("文件生成后移动的目录: {}", ConfigurationManager.getProperty("load_data_workspace") + File.separator + "pool" + File.separator + tableName);
        helper.executeQueryBySql(sql, rs -> {
            logger.info("查询完成,开始处理数据...");
            ResultSetMetaData rsm = rs.getMetaData();
            int colSize = rsm.getColumnCount();
            int fileDataSize = 0;
            int fileCount = 0;
            StringBuffer sb = new StringBuffer();

            while (rs.next()) {
                for (int i = 1; i <= colSize; i++) {
                    int type = rsm.getColumnType(i);
                    if (i < colSize) {
                        sb.append(JdbcUtils.getFieldValue(rs, type, i)).append("\t");
                    } else {
                        sb.append(JdbcUtils.getFieldValue(rs, type, i));
                    }
                }
                sb.append("\r\n");

                fileDataSize++;
                if (fileDataSize >= maxFileDataSize) {
                    String fileName = writeStringToFile(sb, tableName);
                    sb = new StringBuffer();
                    fileCount++;
                    fileDataSize = 0;
                    logger.info("sql返回结果生成第 {} 个数据文件,文件名{}", fileCount, fileName);
                }
            }
            if (sb.length() > 0) {
                String fileName = writeStringToFile(sb, tableName);
                fileCount++;
                logger.info("sql返回结果生成最后一个数据第 {} 个数据文件,文件名{}", fileCount, fileName);
            }
            logger.info("一次查询的数据处理完毕...");
        });

    }

    /**
     * 将查询出来的数据写入到文件
     *
     * @param sb
     */
    private static String writeStringToFile(StringBuffer sb, String tableName) {
        String constantPath = ConfigurationManager.getProperty("load_data_workspace");
        //${basePath}/${workType}/${tableName}/${tableDataFileName}
        String filePathTemplate = constantPath + File.separator     //${basePath}
                + "${dir}" + File.separator         //${workType}
                + tableName + File.separator        //${workType}
                + tableName + "_data_" + DateUtils.STEMP_FORMAT.format(new Date()) + "${random}.tsv"; //${tableDataFileName}

        String createFilePath = filePathTemplate.replace("${dir}", "data");
        String random = RandomStringUtils.randomAlphabetic(8);
        createFilePath = createFilePath.replace("${random}", random);

        File createFile = new File(createFilePath);
        File parentDir = createFile.getParentFile();
        if (!parentDir.exists() || !parentDir.isDirectory()) {
            parentDir.mkdirs();
        }
        File moveFile = new File(filePathTemplate.replace("${dir}", "pool").replace("${random}", random));
        parentDir = moveFile.getParentFile();
        if (!parentDir.exists() || !parentDir.isDirectory()) {
            parentDir.mkdirs();
        }
        try {
            createFile.createNewFile();
            FileUtils.writeStringToFile(createFile, sb.toString(), "utf-8", false);


            createFile.renameTo(moveFile);
            logger.info("已将数据文件移动到处理目录下...");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("生产数据文件失败...");
            System.exit(-1);
        }

        return moveFile.getName();
    }
}
