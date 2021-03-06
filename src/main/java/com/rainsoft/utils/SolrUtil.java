package com.rainsoft.utils;

import com.rainsoft.BigDataConstants;
import com.rainsoft.FieldConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * Created by Administrator on 2017-04-06.
 */
public class SolrUtil {
    private static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);

    /**
     * 根据字段名和字段值把数据写入到SolrInputDocument
     *
     * @param doc        SolrInputDocument
     * @param fieldName  字段名
     * @param fieldValue 字段值
     */
    public static void addSolrFieldValue(SolrInputDocument doc, String fieldName, String fieldValue) {
        //如果字段值为空跳过
        if (StringUtils.isBlank(fieldValue)
                || "null".equalsIgnoreCase(fieldValue))
            return;

        //字段不需要导入到Solr中
        if (FieldConstants.SOLR_FIELD_MAP.get("exclusion_fields").contains(fieldName.toLowerCase()))
            return;

        //字段在Solr里是日期类型
        if (FieldConstants.SOLR_FIELD_MAP.get("date_type_fields").contains(fieldName.toLowerCase())) {
            try {
                Date temp = DateUtils.parseDate(fieldValue, Locale.CHINA, "yyyy-MM-dd HH:mm:ss");
                //如果是capture_time则另外再转转时间戳存储一份
                if (BigDataConstants.CAPTURE_TIME.equalsIgnoreCase(fieldName)) {
                    doc.addField(fieldName.toLowerCase(), temp.getTime());
                }
                doc.addField(fieldName, temp);
            } catch (ParseException e) {  }
        } else if (FieldConstants.SOLR_FIELD_MAP.get("mac_type_fields").contains(fieldName.toLowerCase())) {
            //字段是Mac地址,需要去掉中划线
            fieldValue = fieldValue.replace("-", "");
            doc.addField(fieldName, fieldValue);
        } else {    //字段在Solr里是text类型
            //sid
            if ("id".equalsIgnoreCase(fieldName)) {
                doc.addField("SID", fieldValue);
            } else {
                doc.addField(fieldName, fieldValue);
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
        return hashPrefix + hex;
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
                if (!list.contains(identifier)) {
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
     * @param date     要写入的日期
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
            return identify + year + month + "0" + dayIdentify;
        }
    }

    /**
     * 如果是CloudSolrClient的话设置默认的Collection
     *
     * @param client SolrClient
     * @param obj    Solr Collection 标识，如果是日期类型的就写入到按日期划分的Collection，如果是字符串类型的就直接写入到此字符串
     */
    public static void setCloudSolrClientDefaultCollection(SolrClient client, Object obj) {
        if (client instanceof CloudSolrClient) {
            String collectionName = null;
            if (obj instanceof Date)
                collectionName = createSolrCollectionName("yisou", (Date) obj, 10);
            else if (obj instanceof String)
                collectionName = (String) obj;

            if (null != collectionName) {
                logger.info("当前的Solr Collection为: {}", collectionName);
                ((CloudSolrClient) client).setDefaultCollection(collectionName);
            }
        }
    }

    /**
     * 当要写入Solr的列表达到一定的数量时写入到Solr
     * 如果是集群版Solr的话要根据日期确定写入到哪个Solr的Collection
     *
     * @param client SolrClient客户端，单机版的话为HttpSolrClient，集群版的话为CloudSolrClient
     * @param list   List<SolrInputDocument> 要写入Solr的列表
     * @param size   list的大小达到多少时写入到Solr
     * @param dateOp 可能是String类型，也可能是日期类型，要写入到Solr的列表任一数据的捕获时间，如果是集群版的话程序要根据这个字段决定Solr写入到哪个Collection
     */
    public static void submitToSolr(SolrClient client, List<SolrInputDocument> list, int size, Optional<Object> dateOp)
            throws IOException, SolrServerException {
        Object obj;
        if (!dateOp.isPresent()
                || (null == list)
                || (list.size() == 0)
                || (list.size() < size)) {
            return;
        }

        if (dateOp.get() instanceof Date) {
            obj = dateOp.get();
        } else if (dateOp.get() instanceof String) {
            try {
                obj = DateUtils.parseDate((String) dateOp.get(), "yyyy-MM-dd HH:mm:ss");
            } catch (Exception e) {
                obj = dateOp.get();
            }
        } else {
            return;
        }

        setCloudSolrClientDefaultCollection(client, obj);
        client.add(list, 10000);
        logger.info("写入Solr {} 条数据", list.size());
        list.clear();
    }

    /**
     * Solr多Collection划分，根据日期划分Collection
     * 划分的规则是：
     * 前缀 + 年 + 月 + 日期标识
     * 前缀：一般为Collection不划分的时候的命名
     * 年：4位数字，如：2018
     * 月：2位数字不足以0补，如：08
     * 日标识：日除以Collection存储的时间段，31号和30号相同，避免再单独为一天划分一个Collection
     * Collection默认存储10天的数据，
     *
     * 如2018-08-31这一天所在的Collection应该为：
     * (31 -1) / 10 - 1 = 02
     * 2018-08-30所在的Collection应该为：
     * 30 / 10 - 1 = 02
     * 2018-08-29所在的Collection应该为：
     * 29 / 10 = 02
     * 2018-08-20所在的Coll应该为：
     * 20 / 10 -1 = 01
     * 所以2018年8月会有3个Collection：
     * yisou20180800：存储1号到10号共 10天 的数据
     * yisou20180801：存储11号到20号共 10天 的数据
     * yisou20180802：存储21号到31号共 11天 的数据
     * @param identify 一般为Collection不划分的时候的命名
     * @param date 日期
     * @param period 一个Collection存储指定时间段内的数据
     * @return Collection名称
     */
    public static String getCollection(String identify, Date date, int period) {
        if (null == identify) {
            logger.error("Solr的Collection项目标识为空");
            return identify;
        }
        if (null == date) {
            logger.warn("Solr获取Collection名称时日期为空");
            return identify;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        int year = calendar.get(Calendar.YEAR);

        //月份
        int temp1 = calendar.get(Calendar.MONTH) + 1;
        //月份如果是个位的话前面补零
        String month = temp1 < 10 ? "0" + temp1 : "" + temp1;

        //日
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        //31号按30号算，默认一个Collection存储30天的数据，否则的话会为31号单独划分一个Collection
        day = day > 30 ? day - 1 : day;
        //日除了存储周期
        int temp2 = day / period;
        //如果正好整除，如10号，20号，30号这一天算到前一个Collection里
        temp2 = day % period == 0 ? temp2 - 1 : temp2;
        //日标识是两位，不足的话前面补0
        String count = temp2 < 10 ? "0" + temp2 : "" + temp2;

        String collectionName =identify + year + month + count;
        logger.info("日期 {} 应该存储的Collection为：{}", DateFormatUtils.DATE_TIME_FORMAT.format(date), collectionName);
        return collectionName;
    }

    public static void main(String[] args) {
        Date date = com.rainsoft.utils.DateUtils.stringToDate("2018-01-01 12:34:56", "yyyy-MM-dd HH:mm:ss");
        System.out.println(getCollection("yisou", date, 10));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        System.out.println(" day  = " + calendar.get(Calendar.DAY_OF_MONTH));
        System.out.println("month = " + calendar.get(Calendar.MONTH));
    }
}