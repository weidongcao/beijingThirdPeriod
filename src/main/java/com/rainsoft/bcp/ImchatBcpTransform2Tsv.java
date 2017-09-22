package com.rainsoft.bcp;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import com.rainsoft.conf.ConfigurationManager;
import com.rainsoft.utils.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Ftp类型的Bcp文件转为Tsv
 * Created by CaoWeiDong on 2017-09-21.
 */
public class ImchatBcpTransform2Tsv extends BaseBcpTransform2Tsv {
    private static final String task = "im_chat";
    private static final String[] importColumns = new String[]{""};
    @Override
    public void transformBcpToTsv(String resourcePath, String targetPath) {
        //BCP文件所在目录
        File dir = FileUtils.getFile(resourcePath);
        //BCP文件列表
        File[] files = dir.listFiles();
        //转成TSV文件后最大行数
        int maxFileDataSize = ConfigurationManager.getInteger("data_file_max_lines");
        //Bcp文件字段切分后对应的字段名
        String[] columns = FieldConstants.BCP_FIELD_MAP.get(task);

        //统计BCP文件行数
        int lineCount = 0;
        //写入TSV文件前的缓存
        StringBuffer sb = new StringBuffer();

        //获取要过滤的字段在的字段中的下标
        int[] filterIndex = new int[importColumns.length];
        for (int i = 0; i < importColumns.length; i++) {
            filterIndex[i] = ArrayUtils.indexOf(columns, importColumns[i].toLowerCase());
        }

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
                continue;
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


                //检测此条数据关键字段是否为空，为空的话过滤掉
                if (validColumns(fieldValues, filterIndex)) {
                    continue;
                }
                //捕获时间的毫秒，HBase按毫秒将同一时间捕获的数据聚焦到一起
                long captureTimeMinSecond;
                try {
                    captureTimeMinSecond = DateUtils.TIME_FORMAT.parse(fieldValues[20]).getTime();
                } catch (Exception e) {
                    continue;
                }

                //捕获时间的毫秒+UUID作为数据的ID(HBase的rowKey,Solr的SID, Oracle的ID)
                String uuid = UUID.randomUUID().toString().replace("-", "");
                String rowKey = captureTimeMinSecond + "_" + uuid;
                sb.append(rowKey).append("\t");

                //写入StringBuffer缓存
                for (String fieldValue : fieldValues) {
                    sb.append(fieldValue).append("\t");
                }
                //删除最后的制表符
                sb.deleteCharAt(sb.length() - 1);

                //行数加1
                lineCount++;
                //换行
                sb.append("\r\n");

                //达到最大行数写入TSV文件
                if (lineCount >= maxFileDataSize) {
                    //写入本地文件
                    writeStringToFile(sb, targetPath, task);
                    sb = new StringBuffer();
                    lineCount = 0;
                }
            }
        }
        //写入TSV文件
        if (sb.length() > 0) {
            //write into local file
            writeStringToFile(sb, targetPath, task);
        }
    }
}
