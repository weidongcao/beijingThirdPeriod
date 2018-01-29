package com.rainsoft.utils;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2017-04-06.
 */
public class SolrUtil {
    private static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);

    public static void closeSolrClient(SolrClient client) {
        try {
            if (null != client) {
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void createSolrinputDocumentFromHBase(
            SolrInputDocument doc,
            Result result,
            String[] columns, String CF
    ) {

        for (int i = 1; i < columns.length; i++) {
            String value = Bytes.toString(result.getValue(CF.getBytes(), columns[i].toUpperCase().getBytes()));
            if (StringUtils.isNotBlank(value)) {
                doc.addField(columns[i].toUpperCase(), value);
            }
            if (columns[i].equalsIgnoreCase("capture_time")) {
                try {
                    doc.addField("capture_time", DateFormatUtils.DATE_TIME_FORMAT.parse(value).getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void addBcpIntoSolrInputDocument(
            String[] columns,
            Row row,
            SolrInputDocument doc
    ) {
        for (int i = 0; i < row.length(); i++) {
            if (i >= columns.length) {
                break;
            }
            String value = row.getString(i + 1);
            String key = columns[i].toUpperCase();

            //判断字段是否需要写入到Solr,配置在column.json文件中定义
            String[] exclusionFields = FieldConstants.COLUMN_MAP.get("solr_exclusion_fields");
            if (ArrayUtils.contains(exclusionFields, key.toLowerCase())) {
                continue;
            }
            //如果字段的值为空则不写入Solr
            if (StringUtils.isNotBlank(value)) {
                if (key.equalsIgnoreCase("ending_mac")) {
                    value = value.replace("-", "");
                }
                if (key.equalsIgnoreCase("first_time")
                        || key.equalsIgnoreCase("last_time")
                        || key.equalsIgnoreCase("capture_time")) {
                    try {
                        if (null == value || "".equals(value)) {
                            continue;
                        }
                        value = DateFormatUtils.SOLR_FORMAT.format(DateFormatUtils.DATE_TIME_FORMAT.parse(value));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
                doc.addField(key, value);
            }
        }
    }

    /**
     * 根据字段名和字段值把数据写入到SolrInputDocument
     *
     * @param doc        SolrInputDocument
     * @param fieldName  字段名
     * @param fieldValue 字段值
     */
    public static void addSolrFieldValue(SolrInputDocument doc, String fieldName, String fieldValue) {
        //如果字段值为空跳过
        if (StringUtils.isBlank(fieldValue))
            return;

        //字段不需要导入到Solr中
        if (ArrayUtils.contains(FieldConstants.COLUMN_MAP.get("solr_exclusion_fields"), fieldName.toLowerCase())) {
            return;
        }
        //字段在Solr里是日期类型
        if (ArrayUtils.contains(FieldConstants.COLUMN_MAP.get("solr_date_field_type"), fieldName.toLowerCase())) {
            try {
                Date temp = DateUtils.parseDate(fieldValue, "yyyy-MM-dd HH:mm:ss");
                //如果是capture_time则另外再转转时间戳存储一份
                if (BigDataConstants.CAPTURE_TIME.equalsIgnoreCase(fieldName)) {
                    doc.addField(fieldName.toLowerCase(), temp.getTime());
                }
                doc.addField(fieldName.toUpperCase(), temp);
            } catch (ParseException e) {
                //日期转换失败，什么都不做直接跳过
            }
        } else {    //字段在Solr里是text类型
            //sid
            if ("id".equalsIgnoreCase(fieldName)) {
                doc.addField("SID", fieldValue);
            } else {
                doc.addField(fieldName.toUpperCase(), fieldValue);
            }
        }
    }

    /**
     * 生成HBase的RowKey的前缀
     * RowKey由两部分组成，Rowkey的前缀和唯一标识符
     * 前缀由两部分组成
     * 第一部分: 一个两位的随机数,对数据进行散列
     * 第二部分:
     * 1. 对当前时间的毫秒数进行位运算(相当于除以1024)以减小时间戳的长度（命名为second）
     * 2. 获取比以上值高一个数量级的最小值,比如说second是14亿,它的数量级就是十亿级的，
     * 比它高一个数量级就是百亿级的，百亿级的最小值就是100亿（命名为min）
     * 2. 用min 减去second，从而使最新插入的数据排的前面
     * 3. 将以上结果转为16进制
     * 这样做有以下考虑：
     * 1. 现在的时间戳转秒数按位与运算省去了大量的计算，但是结果是差不多的
     * 2. 将时间戳转为秒数，再转为16进制大大减小了rowkey的长度
     * 3. 当时时间的时间戳是10亿级的，就是再过100年，也不会达到百亿级，这样字符的长度都是固定的
     * 这样做有以下好处：
     * 1. 对HBase进行负载均衡，对数据进行散列，当前插入的数就会随机地分散到每一个Region
     * 2. 对导入时间转时间戳再转秒数然后再转16进制
     * 使rowkey不会过长，但是同时不同Region中同一时间段内的数据都集中到了一起，提高HBase查询的速度
     *
     * @param date 导入时间
     * @return rowkeyPrefix rowkey前缀
     */
    public static String createRowKeyPrefix(Date date) {
        //生成散列
        String hashPrefix = RandomStringUtils.randomAlphabetic(2);
        //根据毫秒数按位与运算，向右移10位，相当于除了1024
        Long second = date.getTime() >> 10;
        //获取second的位数
        String zeros = (second + "").replaceAll("\\d", "0");
        //生成秒数的极大值，比如说当前是1秒，极大值就是10秒，当前值是100秒，极大值就是1000秒，以此类推，也就是说极大值比当前秒数高一个数量级
        Long bigSecond = Long.valueOf("1" + zeros);
        //最大秒秒数送去当前日期的秒数，然后转16进制
        String hex = Long.toHexString(bigSecond - second);

        //生成rowkey前缀
        String rowkeyPrefix = hashPrefix + hex;
        return rowkeyPrefix;
    }

    /**
     * rowkey由两部分组成
     * rowkwy的前缀和唯一性标识符
     * 此唯一性标识符随机生成4位大小写字母及数字组成
     * 这样算起来4位，有1477万多种组合
     * <p>
     * 通过上面的Rowkey前缀和唯一性标识符组合的设计有以下好处：
     * 1. 每一个Rowkey都是唯一的
     * 2. 每一个Rowkey的前两位都是散列字段，后4位都是唯一性标识符，中间9位都是导入时间的转化
     * 3. 避免了热点数据聚焦到同一个Region里
     * 4. Rowkey的长度减小到了15个字节
     *
     * @param list 标识符集合
     * @return 生成list集合中不包含的指定长度的标识符
     */
    public static String createRowkeyIdentifier(List<String> list) {
        String identifier = RandomStringUtils.randomAlphanumeric(4);
        if (null != list) {
            while (list.contains(identifier)) {
                identifier = RandomStringUtils.randomAlphanumeric(4);
                if (list.contains(identifier) != true) {
                    list.add(identifier);
                    break;
                }
            }
        }
        return identifier;
    }

    public static String createRowkey() {
        String prefixRowkey = createRowKeyPrefix(new Date());
        return prefixRowkey + createRowkeyIdentifier(null);
    }

    /**
     * 获取Solr Collection的名称
     * 例如：yisou20180100
     *
     * @param identify 项目标识,一般为yisou
     * @param  date     要写入的日期
     * @param days     一个Collection保存多少天的数据（一般为10天）
     * @return Collection 名称
     */
    public static String createSolrCollectionName(String identify, Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        if (day > 30) {
            day--;
        }
        int dayIdentify = (day - 1) / days;
        if (month < 10) {
            return identify + year + "0" + month + "0" + dayIdentify;
        } else {
            return identify + year +  month + "0" + dayIdentify;
        }
    }

    /**
     * 如果是CloudSolrClient的话设置默认的Collection
     * @param client SolrClient
     * @param date 数据生成的日期
     */
    public static void setCloudSolrClientDefaultCollection(SolrClient client, Date date) {
        if (client instanceof CloudSolrClient) {
            String collectionName = createSolrCollectionName("yisou", date, 10);
            logger.info("当前的Solr Collection为: {}", collectionName);
            ((CloudSolrClient) client).setDefaultCollection(collectionName);
        }
    }

    /**
     * 当SolrInputDocument列表大于指定长度时,把数据写入Solr
     * @param client SolrClient
     * @param list List<SolrInputDocument>
     * @param size List的大小
     * @param date 数据产生的日期
     * @throws IOException
     * @throws SolrServerException
     */
    public static void submitToSolr(SolrClient client, List<SolrInputDocument> list, int size, Date date)
            throws IOException, SolrServerException {
        if (list.size() >= size) {
            setCloudSolrClientDefaultCollection(client, date);
            client.add(list, 10000);
            logger.info("写入Solr {} 条数据", list.size());
            list.clear();
        }
    }

    public static void submitToSolr(SolrClient client, List<SolrInputDocument> list, int size, String str)
            throws IOException, SolrServerException, ParseException {
        Date date = DateUtils.parseDate(str, "yyyy-MM-dd HH:mm:ss");
        submitToSolr(client,  list, size, date);
    }
}