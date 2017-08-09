package com.rainsoft.oracle;

import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.jdbc.JDBCHelper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.ParseException;

/**
 * Created by CaoWeiDong on 2017-08-06.
 */
public class LoadOraclemeta {
    private static final Logger logger = LoggerFactory.getLogger(LoadOraclemeta.class);
    public static void main(String[] args) throws ParseException, IOException {
        String tableName = args[0];

        String metaDirPath = ConfigurationManager.getProperty("load_data_workspace") + File.separator + "meta";
        File metaDir = new File(metaDirPath);
        if (!metaDir.isDirectory() || !metaDir.exists()) {
            metaDir.mkdirs();
        }
        String metaFilePath = metaDirPath + File.separator + tableName;
        File metaFile = new File(metaFilePath);
        JDBCHelper helper = JDBCHelper.getInstance();
        if (metaFile.exists()) {
            metaFile.delete();
        }
        metaFile.createNewFile();
        String simpleSql = "select * from ${tableName}  WHERE ROWNUM = 1";
        String temp = simpleSql.replace("${tableName}", tableName);
        helper.executeQueryBySql(temp, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                ResultSetMetaData rsm = rs.getMetaData();
                int colSize = rsm.getColumnCount();
                StringBuffer sb = new StringBuffer();
                if (rs.next()) {
                    for (int i = 1; i <= colSize; i++) {
                        String fieldName = rsm.getColumnName(i);
                        sb.append(fieldName).append("\r\n");
                        System.out.println("fieldName = " + fieldName);
                    }
                }
                FileUtils.writeStringToFile(metaFile, sb.toString(), "utf-8", false);
            }
        });
//        HDFSUtils.fs.copyFromLocalFile(true, true, new Path(metaFile.getAbsolutePath()), new Path(metaDirPath));
    }
}
