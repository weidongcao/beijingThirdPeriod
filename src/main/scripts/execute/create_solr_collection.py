#!/usr/bin/python
# -*- coding: utf-8 -*-

import calendar
import datetime
import json
import os
import subprocess
import sys
import urllib2
import logging.handlers
import logging

# 脚本使用说明
# 程序的入口在脚本的最下面(__main__)
# 程序执行的方法主要是：main(sys.argv), 其他的方法(Collection的创建、删除；别名的创建、删除),
# 根据情况可以把main(sys.arvg)注释掉,并把需要的方法打开
# 程序执行之前需要先初始化参数
# 初次使用需要根据集群情况给程序传参，参数如下：
# host_name: 可以是Solr集群中的任意一台服务器的主机名或者IP地址
# solr_port: 此服务器Solr的端口号
# numShards: Solr集群节点数（Solr部署了多少台服务器就是多少）
# replicationFactor: 副本数，数据量不大可以指定副本数为1，数据量大的话指定为2

# 部署说明:
# 第一步：
#   将脚本放在/solrCloud/script目录下
# 第二步：
#   根据上面的说明先初始化参数,并把其他的方法注释掉,把main(sys.argv)方法打开
# 第三步：
#   crontab 添加下面的定时任务：
# 0 3 * * * python /solrCloud/script/create_solr_collection.py
# @reboot python /solrCloud/script/create_solr_collection.py

# 定时任务说明:
#   定时执行此任务每天的凌晨3点执行一次
#   服务器重启执行一次

# 创建Collection的规则
# 脚本的功能为Solr每个月10天创建一个Collection
# Collection的命令格式为"yisou年月(天除了10)"
# 例如
# 2017年12月03日在2017年12月的第1个10天，此Collection命名为yisou20171200
# 2017年12月13日在2017年12月的第2个10天，此Collection命名为yisou20171201
# 2017年12月23日在2017年12月的第3个10天，此Collection命名为yisou20171202
# 2017年12月30日31日，就两天，也把它算到第3个10天，命令为yisou20171203

# 指定时间段的Collection创建完成后会再创建一个虚拟的Collection（yisou）:这个Collection指向所有的Collection

# 全局变量
# 如果需要修改全局变量的值的话，不要在这里改，下面有程序初始化方法，
# 在程序的__main__方法里给init方法传参来修改全局变量的值

reload(sys)
# 指定文件的编码格式为UTF-8
sys.setdefaultencoding('utf-8')

# 获取日志对象并指定名称
logger = logging.getLogger(__name__)
# 指定日志打印格式
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
# 指定日志时间戳打印格式
formatter.datefmt = '%Y-%m-%d %H:%M:%S'

# 脚本所在根路径
root_dir = os.path.split(os.path.realpath(__file__))[0]
# 日志文件路径
log_file = '{root_dir}{path_separator}logs{path_separator}create_solr_collection.log'.format(
    root_dir=root_dir,
    path_separator=os.sep
)

# 判断日志所在目录是否存在, 如果不存在的话创建日志所在目录
if os.path.exists(os.path.dirname(log_file)) is False:
    os.makedirs(os.path.dirname(log_file))

# 文件日志
file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=256 * 1024 * 1024, backupCount=16)
# 指定输出到文件的日志的格式
file_handler.setFormatter(formatter)

# 控制台日志
console_handler = logging.StreamHandler(sys.stdout)
# 指定输出到控制台的日志格式
console_handler.setFormatter(formatter)

# 删除所有的handler
logging.getLogger().handlers = []
# 为logger添加的日志处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

# Solr所在的服务器主机名或IP
host_name = "http://cm02.spark.com"
# Solr的端口号
solr_port = 8983
# 分片数
numShards = 3
# 副本数
replicationFactor = 1
# 一个节点最多多少个分片
maxShardsPerNode = 1
# Collection归属
project_identify = "yisou"
# solr conf dir
solr_conf_dir = "yisou"
# solr collection alias for all
collection_alias_all = project_identify + "-all"

# 获取Solr所有Collection的HTTP请求模板
get_all_collection_url_template = """ {host_name}:{solr_port}/solr/admin/collections?action=LIST&wt=json """
# 生成Solr Collection的HTTP请求模板
create_collection_url_template = """{host_name}:{solr_port}/solr/admin/collections?action=CREATE&name={\
collection_name}&numShards={numShards}&replicationFactor={replicationFactor}&maxShardsPerNode={\
maxShardsPerNode}&collection.configName={solr_conf_dir}&wt=json """

# 为所有Collection创建别名的模板
create_alias_url_template = """{host_name}:{solr_port}/solr/admin/collections?action=CREATEALIAS&name=\
{collection_name}&collections={all_collection}&wt=json """

# 删除Solr里Collection的别名
delete_alias_url_template = """ {host_name}:{solr_port}/solr/admin/collections?action=\
DELETEALIAS&name={alias_name}&wt=json"""

# 测试，创建Collection成功的返回结果
data_create_collection_success = """
{"responseHeader":{"status":0,"QTime":6400},"success":\
{"cm02.spark.com:8080_solr":{"responseHeader":{"status":0,"QTime":1722},\
"core":"yisou20171101_shard2_replica1"},"cm03.spark.com:8080_solr":\
{"responseHeader":{"status":0,"QTime":1734},"core":"yisou20171101_shard3_replica1"}\
,"cm01.spark.com:8080_solr":{"responseHeader":{"status":0,"QTime":1725},\
"core":"yisou20171101_shard1_replica1"}}}
"""

delete_collection_url_template = """
{host_name}:{solr_port}/solr/admin/collections?action=DELETE&name={collection}&wt=json
"""


# 执行执行Linux命令
def exec_cmd(full_command, cwd_path):
    """
    函数功能说明：Python执行Shell命令并返回命令执行结果的状态、输出、错误信息、pid

    第一个参数(full_command)：完整的shell命令
    第二个参数(pwd_path)：执行此命令所在的根目录

    :param full_command: 要执行的Shell命令
    :param cwd_path: 指定执行Shell命令时所在的根目录
    :return: Shell命令执行结果
    返回结果：
        stdout:执行Shell命令的输出
        stderr:执行Shell命令的错误信息
        return_code:执行Shell命令结果的状态码
        pid:执行Shell程序的pid
    """
    try:
        process = subprocess.Popen(full_command, cwd=cwd_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, shell=True)

        while True:
            logger.info(process.stdout.readline(), )
            if process.poll() is not None:
                break
        logger("")
        # 等命令行运行完
        process.wait()

        # 获取命令行输出
        stdout = process.stdout.read()

        # 获取命令行异常
        stderr = process.stderr.read()
        # print("stderr = " + str(stderr))

        # 获取shell 命令返回值,如果正常执行会返回0, 执行异常返回其他值
        return_code = process.returncode
        # logger.info("return_code = " + str(return_code))

        # 获取命令运行进程号
        pid = process.pid

        result_dict = {"stdout": stdout, "stderr": stderr, "return_code": return_code, "pid": pid}
        return result_dict
    except Exception, e:
        logger.error("执行失败", exc_info=True)
        os._exit(0)

        return False


def exec_http_request(url):
    """
    发起HTTP请求
    :param url:  向Solr发起的HTTP请求
    :return: HTTP请求返回的Response
    """
    req = urllib2.Request(url)
    try:
        response = urllib2.urlopen(req).read()
        ss = response.encode('utf-8')
        return json.loads(ss)
    except Exception, e:
        logger.error('执行curl失败：%s', url.__str__())
        logger.error('执行curl失败', exc_info=True)
        os._exit(0)
        return False


def get_collection_name_by_date(key, cur_date):
    """
    由当前日期根据指定规则生成Collection名称,如:yisou20171200,表示项目类型为yisou,
    这个Collection里存储的是2017年12月1日到2017年12月10日的数据

    :param key: Solr Collection前缀, 一般为项目标识
    :param cur_date: 当前时间格式为2018-01-01
    :return: Solr Collection 名称
    """
    # 获取年份
    cur_year = cur_date.strftime("%Y")
    # 获取月份
    cur_month = cur_date.strftime("%m")
    # 获取日
    cur_day = cur_date.strftime("%d")

    # 如果日期大于等于30号不再单独创建Collection
    if int(cur_day) >= 30:
        cur_day = str(int(cur_day) - 5)

    # 10天创建一个一个Collection，如1-10号，11-20号，21-最后
    identify = (int(cur_day) - 1) / 10
    if len(str(identify)) == 1:
        identify = "0" + str(identify)

    # Collection的名称由项目标识+年+月+时间段标识
    collection_name = key + cur_year + cur_month + identify
    logger.info("%s 所在的Collection: %s", cur_date.strftime("%Y-%m-%d"), collection_name)

    return collection_name


def get_previous_collection_by_name(collection_name):
    """
    根据当前Collection的名称获取到前一Collection的名称(yisou20171200)

    :param collection_name: Solr Collection名称
    :return:  根据规则前一个Solr Collection名称
    """
    # 获取日期标识
    identify = collection_name[-2::1]
    # 获取月份
    cur_month = collection_name[-4:-2:1]
    # 获取年份
    cur_year = collection_name[-8:-4:1]

    # 如果日标识为00则上一个一个Collection是上一个月的
    if str(identify) == "00":
        identify = "02"
        # 如果月份是01，则上一个一个Collection的月份标识是上一年的
        if str(cur_month) == "01":
            cur_year = str(int(cur_year) - 1)
            cur_month = "12"
        else:
            cur_month = str(int(cur_month) - 1)
    else:
        identify = str(int(identify) - 1)

    # 统一月份和日标识的长度
    if len(str(identify)) == 1:
        identify = "0" + str(identify)

    if len(cur_month.__str__()) == 1:
        cur_month = "0" + str(cur_month)

    # 得到前一个Collection的名称
    previous_collection_name = collection_name[:-8:1] + cur_year + cur_month + str(identify)

    logger.debug("%s的前一Collection名称: %s", collection_name, previous_collection_name)

    return previous_collection_name


def get_period_by_date(cur_date):
    """
    根据当前日期获取Collection要保存的数据的时间段

    :param cur_date: 当时日期，格式为2018-01-01
    :return: 时间段[开始日期, 结束日期]
    """
    # 获取到日期的日(一个月的第几天)
    cur_day = cur_date.day

    # 一个月的第一天
    first_date = cur_date + datetime.timedelta(days=(-cur_day + 1))

    # 按月10天创建一个一个Collection，如1号到10号创建一个一个Collection
    # 11号到20号创建一个一个Collection
    # 21号到最后创建一个一个Collection
    # 几号就从1号开始向后偏移对应的天数
    if 0 < cur_day <= 10:
        start_date = first_date
        end_date = first_date + datetime.timedelta(days=(10 - 1))
    elif 10 < cur_day <= 20:
        start_date = first_date + datetime.timedelta(days=10)
        end_date = first_date + datetime.timedelta(days=(20 - 1))
    else:
        start_date = first_date + datetime.timedelta(days=20)
        end_date = get_last_day_of_month(cur_date)

    # Collection的开始时间
    start_date_str = datetime.datetime.strftime(start_date, "%Y-%m-%d")

    # Collection的结束时间
    end_date_str = datetime.datetime.strftime(end_date, "%Y-%m-%d")

    logger.debug("当前的日期为: %s ,所在的时间段为：%s 到 %s", cur_date, start_date_str, end_date_str)

    return [start_date_str, end_date_str]


def get_period_by_collection_name(collection_name):
    """
    根据Collection名称获取它所存储的数据的时间段

    :param collection_name: Solr Collection 名称
    :return: [开始日期, 结束日期]
    """
    # 获取Collection名称的时间标识
    date_identify = collection_name[-8::1]

    # 获取日标识
    identify = int(collection_name[-1::1])

    # 获取该时间段内的任意一天
    date_identify = str(int(date_identify) + 5 + (identify * 10))
    # 字符串转为日期
    date_period = datetime.datetime.strptime(date_identify, "%Y%m%d")
    start_end_period = get_period_by_date(date_period)

    logger.debug(
        "%s 的Solr Collection存储的数据所在的时间段为: %s 到 %s",
        collection_name, start_end_period[0], start_end_period[1]
    )

    return get_period_by_date(date_period)


def get_last_day_of_month(cur_date):
    """
    根据日期获取所在月的最后一天
    :param cur_date: 日期,格式为2018-01-01
    :return: 指定日期所在月的最后一天
    """

    cur_year = cur_date.year
    cur_month = cur_date.month
    cur_day = cur_date.day

    # 获取这个月的多少天
    days = calendar.monthrange(cur_year, cur_month)[1]

    # 在当前日期的基础上加上对应的天数
    return cur_date + datetime.timedelta(days=(days - cur_day))


def get_forward_collection_name(collection_name):
    """
    根据Collection名称获取到下一个Collection的名称
    :param collection_name: Solr Collection名称
    :return: 下一个Solr Collection名称
    """
    # 获取日标识
    identify = collection_name[-2::1]
    # 获取月份
    cur_month = collection_name[-4:-2:1]
    # 获取年份
    cur_year = collection_name[-8:-4:1]

    # 如果日标识为02，表示这个Collection存储的是一个月最后10天的数据，
    # 下一个一个Collection就是下一个月的了
    if str(identify) == "02":
        # 重新开始日标识
        identify = "00"
        # 如果是12月，则下一月就过年了
        if str(cur_month) == "12":
            cur_year = str(int(cur_year) + 1)
            cur_month = "1"
        else:
            cur_month = str(int(cur_month) + 1)
    else:
        identify = str(int(identify) + 1)

    # 统一月份和日标识的长度为2，如果不够的话前面补0
    if len(str(identify)) == 1:
        identify = "0" + str(identify)

    if len(str(cur_month)) == 1:
        cur_month = "0" + str(cur_month)

    # 生成下一个一个Collection的名称
    forward_collection_name = collection_name[:-8:1] + cur_year + cur_month + identify
    logger.debug("%s 的下一时间段的Collection为: %s ", collection_name, forward_collection_name)

    return forward_collection_name


def get_earliest_collection_name(collections):
    """
    根据Solr Collection列表获最早的Collection
    :param collections: Solr Collection列表
    :return: Collection列表中最早的Collection
    """
    # 要返回的最早的Solr Collection
    earliest_collection = None

    # 如果Solr Collection列表为空，或者列表内没有元素，直接返回
    if (collections is None) or (collections.__length__() == 0):
        return None

    # 获取Collection中最早的Collection
    for collection in collections:
        if earliest_collection is None:
            earliest_collection = collection
        else:
            earliest_date_identify = earliest_collection[-8::1]
            date_identify = collection[-8::1]
            if int(date_identify) < int(earliest_date_identify):
                earliest_collection = collection

    return earliest_collection


def get_all_collection():
    """
    获取Solr所有Collection
    :return: Solr Collection列表
    """
    get_all_collection_url = get_all_collection_url_template.format(host_name=host_name, solr_port=solr_port.__str__())

    logger.info("正在获取Solr所有的Collection ...")

    # 去Solr集群查询
    response = exec_http_request(get_all_collection_url)
    collections = response["collections"]

    # 查询返回的结果是unicode编码，转为string
    col_utf8 = []
    for col in collections:
        collection_name = col.encode("utf-8")
        # 过滤非project_identify标识的Collection
        if project_identify in collection_name:
            col_utf8.append(collection_name)
    logger.info("获取成功, Solr已创建的所有Collection为: %s", col_utf8.__str__())

    return col_utf8


def get_collection_start_date(collection_name):
    """
    Solr Collection 是按时间段划分的
    根据Solr Collection的名称获取它存储的数据的时间段的开始日期,格式为：yyyy-mm-dd
    :param collection_name: Solr Collection名称
    :return: 开始日期
    """
    # 获取Collection名称的时间标识
    date_identify = collection_name[-8::1]
    date_identify = date_identify[0:-1:1] + "0"

    # 获取日标识
    identify = int(collection_name[-1::1])

    # 获取该时间段内的任意一天
    date_identify = str(int(date_identify) + 1 + (identify * 10))
    # 字符串转为日期
    start_date = datetime.datetime.strptime(date_identify, "%Y%m%d")

    return start_date


def get_collection_end_date(collection_name):
    """
    Solr Collection 是按时间段划分的
    根据Solr Collection的名称获取它存储的数据的时间段的结束日期,格式为：yyyy-mm-dd
    :param collection_name: Solr Collection名称
    :return: 结束日期
    """
    # 先获取到此Solr Collection时间段的第一天
    start_date = get_collection_start_date(collection_name)
    # 获取日标识
    identify = int(collection_name[-1::1])
    if identify == 2:
        return get_last_day_of_month(start_date)
    else:
        return start_date + datetime.timedelta(days=(10 - 1))


def get_collections_by_start_end_date(start_date_str, end_date_str):
    """
    Solr Collection是按时间段划分的,
    根据开始日期和结束日期获取它们之间的Solr Collection
    如果Solr Collection包含的时间段不完整的话剔除
    :param start_date_str: 开始日期
    :param end_date_str: 结束日期
    :return: Solr Collection列表
    """
    # 开始和结束日期之间的Solr Collection列表
    collections = []
    # 开始日期所在的Solr Collection名称
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    start_collection_name = get_collection_name_by_date(project_identify, start_date)
    # 结束日期所在的Solr Collection名称
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
    end_collection_name = get_collection_name_by_date(project_identify, end_date)

    # 如果开始日期与Collection的开始日期一致,将开始日期所在的Collection添加到列表
    if start_date.date() == get_collection_start_date(start_collection_name).date():
        collections.append(start_collection_name)

    # 从开始的Collection向后获取Collection,一直到结束的Collection
    while True:
        forward_collection_name = get_forward_collection_name(start_collection_name)
        # 如果是最后一个Collection,跳循环
        if forward_collection_name == end_collection_name:
            break
        else:
            # 否则的话将Collection添加到列表
            collections.append(forward_collection_name)
            start_collection_name = forward_collection_name

    # 如果结束日期与Collection的结束日期一致,将结束日期所在的Collection添加到列表
    if end_date.date() == get_collection_end_date(end_collection_name).date():
        collections.append(end_collection_name)

    return collections


def create_previous_collection(collections, cur_collection_name):
    """
    创建前一个Collection
    递归创建前一个Collection，如果前一个Collection没有创建的话，后面所有的Collection都不能创建
    这样所有的Collection就都创建了
    :param collections: Solr Collection列表
    :param cur_collection_name: 当前Solr Collection名称
    :return:是否创建成功
    """
    # 判断当前Collection是否已经创建
    if cur_collection_name not in collections:
        # 获取前一个Collection的名称
        prev_collection = get_previous_collection_by_name(cur_collection_name)

        # 递归创建前一个Collection，如果前一个Collection已经创建直接返回True
        create_previous_collection_result = create_previous_collection(collections, prev_collection)

        # 如果前一个一个Collection已经创建成功才创建下一个Collection
        if create_previous_collection_result:

            # 执行创建Collection
            create_cur_collection_result = create_collection(cur_collection_name)

            # 如果Collection创建成功把这个Collection添加到列表并返回
            if create_cur_collection_result:
                collections.append(cur_collection_name)
                return True
            else:
                return False
    else:
        return True


def create_previous_collection_until_end_date(collections, cur_collection_name, start_date):
    """
    从指定的时间开始创建Collection,一直创建到当前的Collection
    本来想用重载的，但是Python不支持重载
    参数说明：
      collections     要创建的Collection列表
      cur_collection_name 当前的Collection的名称
      start_date      指定开始创建Collection的日期
    相当于为程序指定一个开始和结束的Solr Collection，然后再指定这中间有哪些Solr Collection已经创建了，
    然后把没有创建的Collection从前往后都创建一下
    比如说start_date 为2017-09-01, cur_collection_name为yisou20180100,
    collections为[yisou20171201,yisou20171202]
    则创建从2017-09-01开始，除了yisou20171201,yisou20171202外，从yisou20170900到20180100的所有的Solr Collection
    如果没有传开始创建的日期则默认从当前日期开始

    :param collections: Solr Collection 列表
    :param cur_collection_name: 当前Solr Collection名称
    :param start_date: 开始日期,格式为2018-01-01
    :return: Solr Collection是否创建成功
    """
    if start_date is None:
        start_date = datetime.datetime.now()
        logger.info(
            "没有设置开始创建的日期，默认从当前日期开始创建，当时日期为: %s",
            datetime.datetime.strftime(start_date, "%Y-%m-%d")
        )

    # 获取当前日期的Collection的名称
    start_collection_name = get_collection_name_by_date(project_identify, start_date)

    # 获取当前日期的Collection的前一个Collection
    previous_start_collection_name = get_previous_collection_by_name(start_collection_name)

    # 将前一个Collection添加到列表，
    # 程序递归创建Solr Collection的时候需要一起截止标识，来判断到什么时候停止
    collections.append(previous_start_collection_name)

    # 递归创建Collection到当前时间
    create_status = create_previous_collection(collections, cur_collection_name)

    # 把当前日期的Collection的前一个Collection从列表中移除，这个Solr Collection
    # 后面还要根据这些Collection列表为它们创建别名
    collections.remove(previous_start_collection_name)

    return create_status


def create_collection(collection_name):
    """
    创建Collection
    :param collection_name: Solr Collection名称
    :return: Solr Collection是否创建成功
    """
    # 替换创建模板
    create_collection_url = create_collection_url_template.format(
        host_name=host_name,
        solr_port=solr_port.__str__(),
        numShards=numShards.__str__(),
        replicationFactor=replicationFactor.__str__(),
        maxShardsPerNode=maxShardsPerNode.__str__(),
        collection_name=collection_name,
        solr_conf_dir=solr_conf_dir
    )

    logger.info("开始创建Collection: %s ... ", collection_name)

    # 开始创建Collection
    response = exec_http_request(create_collection_url)
    # response = json.loads(data_create_collection_success)

    # 获取此Collection要存储的数据的时间段
    start_end = get_period_by_collection_name(collection_name)

    # 判断是否创建成功
    if "success" in str(response):
        logger.info("已为 %s 到 %s 期间的数据创建Solr Collection: %s", start_end[0], start_end[1], collection_name)
        return True
    else:
        logger.error("创建 %s 到 %s 期间的Solr Collection : %s 失败", start_end[0], start_end[1], collection_name)
        return False


def delete_collections(collections):
    """
    删除Collection
    :param collections: Solr Collection列表
    :return: 不返回
    """
    # 如果Solr集群中根本没有没有Collection直接返回
    if collections.__len__() == 0:
        logger.info("没有需要删除的Collection")
        return

    # 循环删除列表里的Collection
    for collection in collections[0:]:

        # 替换要删除的Collection
        delete_collection_url = delete_collection_url_template.format(
            host_name=host_name,
            solr_port=solr_port.__str__(),
            collection=collection
        )

        # 执行删除
        response = exec_http_request(delete_collection_url)

        # 判断是否删除成功，这个删除成功了才能删除下一个
        if "success" in str(response):
            logger.info("Collection删除成功： %s", collection.__str__())
            # 从Solr Collection列表中移除
            collections.remove(collection)

        # logger.info("response = " + str(response))


def delete_collections_by_start_end_day(start_date, end_date):
    """
    根据开始日期和结束日期删除指定时间段之间的所有Solr Collection
    如果中间有时间段不完整的Solr Collection,不完整的Solr Collection不删除
    例如,指定时间段为2017-12-05到2017-01-05,
    那么会删除
    2017-12-11到2017-12-20的Solr Collection: yisou20171201
    2017-12-21到2017-12-31的Solr Collection: yisou20171202
    而不会删除的是:
    2017-12-05到2017-12-10的Solr Collection: yisou20171200,因为它还包含2017-12-01到2017-12-04的数据
    2018-01-01到2017-01-05的Solr Collection: yisou20180100,因为它还包含2018-01-06到2018-01-10的数据

    :param start_date: 开始日期,格式为2018-01-01
    :param end_date: 结束日期,格式为2018-01-01
    :return:
    """
    # 指定日期所有的Solr Collection的名称
    start_end_collections = get_collections_by_start_end_date(start_date, end_date)
    # Solr 已经创建的所有的Collection名称
    all_solr_collections = get_all_collection()

    # 判断指定日期内的Collection是否包含在Solr已经创建的所有的Collection,如果没有的话剔除
    for collection_name in start_end_collections[0:]:
        if not all_solr_collections.__contains__(collection_name):
            start_end_collections.remove(collection_name)

    # 删除指定时间段内的所有Collection
    delete_collections(start_end_collections)


def delete_all_collections():
    """
    删除所有Collection
    :return: 不返回
    """
    collections = get_all_collection()
    delete_collections(collections)


def create_alias(alias_name, collections):
    """
    为指定的Solr Collection列表Collection创建别名

    :param alias_name: Solr Collection别名
    :param collections: Solr Collection列表
    :return: 是否创建Solr另外成功
    """
    # 替换请求模板参数
    create_alias_url = create_alias_url_template.format(
        host_name=host_name,
        solr_port=solr_port.__str__(),
        all_collection=','.join(collections),
        collection_name=alias_name
    )

    # 先执行删除Solr Collection 别名
    delete_alias(alias_name)

    # 为Solr指定的Collection 列表创建别名
    logger.info("为Solr指定的Collection创建别名: %s", alias_name)
    logger.info("%s 所指向的Collections: %s", alias_name, collections.__str__())
    response = exec_http_request(create_alias_url)
    # response = json.loads(data_create_collection_success)

    # 判断是否删除成功
    if int(response["responseHeader"]["status"]) == 0:
        logger.info("为Solr Collections创建别名成功")
        return True
    else:
        logger.error("创建别名失败")
        return False


def create_alias_for_all(alias_name):
    collections = get_all_collection()
    create_alias(alias_name, collections)


def delete_alias(alias_name):
    # 替换请求模板参数
    delete_alias_url = delete_alias_url_template.format(
        host_name=host_name,
        solr_port=solr_port.__str__(),
        alias_name=alias_name
    )

    # 执行删除
    logger.info("即将删除Solr Collection 别名: %s", alias_name)
    req = urllib2.Request(delete_alias_url)
    try:
        response = urllib2.urlopen(req).read()
        ss = response.encode('utf-8')
        return json.loads(ss)
    except Exception, e:
        # Solr里根本没有这个错误引起的异常
        if e.__str__().__contains__("500"):
            logger.info("%s 要删除的别名还没有创建")
        # 其他未知的异常
        else:
            logger.error('执行curl失败：%s', delete_alias_url.__str__())
            logger.error('执行curl失败', exc_info=True)
            os._exit(0)

    return False


def init(hostname, port, shards, replicas):
    """
    初始化参数
    :param hostname: 主机名或IP地址
    :param port: Solr服务的端口号
    :param shards: Solr分片数
    :param replicas: Solr分片副本数
    :return: 不返回
    """
    # 指定全局变量
    global host_name, solr_port, numShards, replicationFactor, maxShardsPerNode

    # 主机名
    host_name = hostname
    # 端口号
    solr_port = port
    # 分片数
    numShards = shards
    # 副本数
    replicationFactor = replicas
    # 单个节点最大分片数
    maxShardsPerNode = replicas


def main(args):
    """
    主程序
    :param args: 命令行传的参数列表,用于指定开始创建Solr Collection的日期,格式为2018-01-01
    :return: 不返回
    """
    # 当前日期
    cur_date = datetime.datetime.now()
    # 根据规则生成当前时间段的Collection的名称
    cur_collection_name = get_collection_name_by_date(project_identify, cur_date)

    # 获取Solr集群中所有的Collection
    collections = get_all_collection()

    # 如果传传日期的参数从所传日期开始创建Collection
    if args.__len__() > 1:
        start_date = datetime.datetime.strptime(args[1], "%Y-%m-%d")
        create_previous_collection_until_end_date(collections, cur_collection_name, start_date)
        return

    # 根据当前时间段的Collection的名称取得下一个时间段的Collection名称
    forward_collection_name = get_forward_collection_name(cur_collection_name)

    # Solr中是否有Collection，如果有的话先判断判断Collection是否已经创建
    create_collection_result = False
    if collections.__len__() > 0:
        # 判断下一Collection是否已经创建，如果已经创建，直接返回，如果没有创建，先判断前面的Collection是否已经创建
        if forward_collection_name in collections:
            logger.info("当前时间段及下一时间段的Collection都已经创建，不需要再创建")
        else:
            # 如果Solr里已经有Collection，判断前面的Collection是否已经创建, 并创建当前时间段和下一时间段的Collection
            create_collection_result = create_previous_collection(collections, forward_collection_name)
    else:
        # 如果Solr里一个Collection都没有，则创建当前时候段的Collection及下一时间段的Collection
        create_collection_result = create_previous_collection_until_end_date(collections, forward_collection_name, None)

    # 为所有Collection创建别名
    if create_collection_result:
        # 为所有的Collection创建别名
        create_alias(project_identify + "-all", collections)


if __name__ == "__main__":
    """ 程序入口 """
    # 初始化参数
    # 第一个参数：Solr集群中任意一台服务器的主机名或IP地址
    # 第二个参数：该服务器中Solr服务对应的商品号
    # 第三个参数：Solr集群中的节点数
    # 第四个参数：Solr副本数(数据量小为1，数据量大为2)
    init("http://cm02.spark.com", 8983, 3, 2)

    # 定时创建Solr Collection主程序
    # sys.argv用于指定从指定的日期开始创建Solr Collection,一直创建到当前时间
    main(sys.argv)

    # 为所有Solr Collection创建别名
    # create_alias_for_all(collection_alias_all)
    # 删除指定的Solr Collection别名
    # delete_alias(collection_alias_all)
    # 删除所有的Solr Collection
    # delete_all_collections()

    # 删除从指定的开始结束日期之间的Solr Collection,如果指定的开始或结束日期
    # 不包含完整的Solr Collection,不完整的Solr Collection不删除
    # delete_collections_by_start_end_day("2017-06-10", "2017-10-01")

    # 创建Solr Collection
    # create_collection(project_identify + "20180100")

