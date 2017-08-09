#!/usr/bin/python
# coding=utf-8

import datetime
import subprocess
import sys
import time
import os

# 按天导入的表
day_cmd_template_dict = {
    # "bbs_class": "BbsOracleDataCreateSolrIndex",
    # "email_class": "EmailOracleDataCreateSolrIndex",
    "ftp_class": "FtpOracleDataCreateSolrIndex",
    "imchat_class": "ImchatOracleDataCreateSolrIndex",
    # "search_class": "SearchOracleDataCreateSolrIndex",
    # "shop_class": "ShopOracleDataCreateSolrIndex",
    # "weibo_class": "WeiboOracleDataCreateSolrIndex"
}

# 一天分多次导入的表
percent_cmd_template_dict = {"http_class": "HttpOracleDataCreateSolrIndex"}

pref_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr."
suffix_date = " ${cur_date}"
suffix_percent = " ${start_percent} ${end_percent}"


def exec_command_shell(full_command, cwd_path):
    """
    函数功能说明：Python执行Shell命令并返回命令执行结果的状态、输出、错误信息、pid

    第一个参数(full_command)：完整的shell命令
    第二个参数(pwd_path)：执行此命令所在的根目录

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
            print process.stdout.readline() ,
            if process.poll() is not None:
                break
        print("")
        # 等等命令行运行完
        process.wait()

        # 获取命令行输出
        stdout = process.stdout.read()

        # 获取命令行异常
        stderr = process.stderr.read()
        # print("stderr = " + str(stderr))

        # 获取shell 命令返回值,如果正常执行会返回0, 执行异常返回其他值
        return_code = process.returncode
        # print("return_code = " + str(return_code))

        # 获取命令运行进程号
        pid = process.pid

        result_dict = {"stdout": stdout, "stderr": stderr, "return_code": return_code, "pid": pid}
        return result_dict
    except Exception as e:
        print(e.message)
        return False

def format_print(content):
    suffex_info = "[" + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S") + " INFO] "

    print(suffex_info + content)


def run_shell(cmd, pwd_path):
    """
    执行Linux Shell命令，如果执行失败退出程序
    :param cmd: Shell命令
    :param pwd_path: 执行Shell命令时所在的目录
    :return:
    """
    result_dict = exec_command_shell(cmd, pwd_path)
    # result_dict = {"return_code": 0}
    if result_dict["return_code"] != 0:
        print("程序执行失败,程序即将退出")
        os._exit(0)
    # 休眠, 等待内存清理完毕
    time.sleep(1)


def run_shell_by_day(template_cmd, cur_date, pwd_path):
    """
    按天执行Linux Shell命令
    :param template_cmd: 命令模板
    :param cur_date: 日期参数
    :param pwd_path: 执行Shell命令时所在的目录
    :return:
    """
    cmd = template_cmd.replace("${cur_date}", cur_date)

    format_print("当前要执行的命令：" + cmd)

    run_shell(cmd, pwd_path)


def run_shell_by_day_percent(template_cmd, cur_date, run_count, pwd_path):
    """
    一天的数据分多次执行Shell命令
    :param template_cmd: 命令模板
    :param cur_date: 日期参数
    :param run_count: 一天的数据分几次执行
    :param pwd_path: 执行Shell命令时所在的目录
    :return:
    """
    for num in range(run_count):
        cmd = template_cmd.replace("${cur_date}", cur_date)
        cmd = cmd.replace("${start_percent}", str(float(num) / run_count))
        cmd = cmd.replace("${end_percent}", str(float(num + 1) / run_count))

        format_print("当前要执行的命令：" + cmd)
        run_shell(cmd, pwd_path)


def valid_date(date_param):
    """判断是否是一个有效的日期字符串"""
    try:
        time.strptime(date_param, "%Y-%m-%d")
        return True
    except:
        return False


def valid_param(args):
    """参数校验"""
    if not valid_date(args[1]):
        format_print("开始日期参数不是合法的日期: " + args[1])
        os._exit(0)

    if not valid_date(args[2]):
        format_print("结束日期参数不是合法的日期: " + args[2])
        os._exit(0)
    # 开始日期
    start_date = datetime.datetime.strptime(args[1], '%Y-%m-%d')
    # 结束日期
    end_date = datetime.datetime.strptime(args[2], '%Y-%m-%d')

    if start_date < end_date:
        format_print("开始日期必须大于结束日期,开始日期为：" + start_date.__str__() + "; 结束日期为： " + end_date.__str__())
        os._exit(0)


def main(args):
    # 校验参数
    valid_param(args)

    # 开始日期
    start_date = datetime.datetime.strptime(args[1], '%Y-%m-%d')
    # 结束日期
    end_date = datetime.datetime.strptime(args[2], '%Y-%m-%d')
    # 根目录
    pwd_path = os.getcwd()

    """指执行命令"""
    while start_date >= end_date:
        # 导入一天的数据程序开始时间
        sys_start_time = datetime.datetime.now()
        cur_date = start_date.strftime('%Y-%m-%d')

        # 一天导入一次的任务
        for cmd in day_cmd_template_dict.values():
            # 拼接命令模板
            template_cmd = pref_cmd + cmd + suffix_date
            # 执行导入
            run_shell_by_day(template_cmd, cur_date, pwd_path)

        # 一天的数据分多次导入的任务
        for cmd in percent_cmd_template_dict.values():
            # 拼接命令模板
            template_cmd = pref_cmd + cmd + suffix_date + suffix_percent
            # 执行导入
            run_shell_by_day_percent(template_cmd, cur_date, 4, pwd_path)

        # 导入一天的数据程序结束时间
        sys_end_time = datetime.datetime.now()

        # 导入一天的数据程序消耗的时间
        format_print("<------------------------- " + cur_date + "的数据索引完毕,耗时：" + str(sys_end_time - sys_start_time).split(".")[0] + " ------------------------->")

        # 日期向前推进一天
        start_date = start_date + datetime.timedelta(days=-1)


if __name__ == "__main__":

    # 开始日期 默认从昨天开始导入
    start_date_param = datetime.datetime.now() + datetime.timedelta(days=-1)

    # 是否要导入实时数据
    real_time_import = True

    # 数据库保留1年的数据
    data_store_days = 365

    """
    逻辑：
    历史的和实现的一起导入
    1点的时候执行实时的数据
    其他时间段执行历史的数据，如果历史的数据索引完了则程序休眠1小时
    """
    while True:
        # 获取当前时间的小时
        cur_time = datetime.datetime.now()
        hour = cur_time.hour
        # 导入实时的数据
        if hour < 3 and real_time_import:
            # 每天的前3个小时导入前一天的数据
            yesterday = cur_time + datetime.timedelta(days=-1)
            # 获取昨天的日期
            yest_date = datetime.datetime.strftime(yesterday, "%Y-%m-%d")
            format_print("start_date = " + yest_date)
            # 参数
            param_list = ['startup-oracle_to_solr.py', yest_date, yest_date]
            # 导入主程序
            main(param_list)
            # 不需要再导入实时数据
            real_time_import = False
        # 导入历史数据或休眠
        else:
            # 索引历史数据
            if data_store_days > 0:
                # 历史数据日期
                cur_date_param = datetime.datetime.strftime(start_date_param, "%Y-%m-%d")

                # 历史数据参数
                param_list = ['startup-oracle_to_solr.py', cur_date_param, cur_date_param]

                # 导入主程序
                main(param_list)

                # 日期向前推进一天
                start_date_param = start_date_param + datetime.timedelta(days=-1)
                data_store_days -= 1
            # 如果历史数据已经索引完毕则程序休眠一个小时
            else:
                format_print("程序休眠 60 秒")
                time.sleep(60)

        # 每天3小时后重置实时数据导入状态
        if hour >= 3 and real_time_import == False:
            real_time_import = True


