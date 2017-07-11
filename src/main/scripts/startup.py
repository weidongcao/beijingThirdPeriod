#!/usr/bin/python
# coding=utf-8

import datetime
import subprocess
import sys
import time
import os


template_bbs_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.BbsOracleDataCreateSolrIndex ${cur_date}"
template_email_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.EmailOracleDataCreateSolrIndex ${cur_date}"

template_ftp_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.FtpOracleDataCreateSolrIndex ${cur_date}"
template_http_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex ${cur_date} ${start_percent} ${end_percent}"
template_imchat_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ImchatOracleDataCreateSolrIndex ${cur_date}"

template_real_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.RealOracleDataCreateSolrIndex ${cur_date}"
template_search_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.SearchOracleDataCreateSolrIndex ${cur_date}"
template_service_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ServiceOracleDataCreateSolrIndex ${cur_date}"
template_shop_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ShopOracleDataCreateSolrIndex ${cur_date}"
template_vid_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.VidOracleDataCreateSolrIndex ${cur_date}"
template_weibo_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.WeiboOracleDataCreateSolrIndex ${cur_date}"
template_test_cmd = "python TestPython.py {start_date} {end_date}"


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
            print process.stdout.readline()
            if process.poll() is not None:
                break

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


def run_shell(cmd, pwd_path):
    """
    执行Linux Shell命令，如果执行失败退出程序
    :param cmd: Shell命令
    :param pwd_path: 执行Shell命令时所在的目录
    :return:
    """
    result_dict = exec_command_shell(cmd, pwd_path)
    if result_dict["return_code"] != 0:
        print("程序执行失败,程序即将退出")
        os._exit(0)
    # 休眠, 等待内存清理完毕
    time.sleep(5)


def run_shell_by_day(template_cmd, cur_date, pwd_path):
    """
    按天执行Linux Shell命令
    :param template_cmd: 命令模板
    :param cur_date: 日期参数
    :param pwd_path: 执行Shell命令时所在的目录
    :return:
    """
    cmd = template_cmd.replace("${cur_date}", cur_date)
    print("当前要执行的命令：" + cmd)
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
    # 休眠5秒, 等待内存清理完毕
    for num in range(run_count):
        cmd = template_cmd.replace("${cur_date}", cur_date)
        cmd = cmd.replace("${start_percent}", str(float(num) / run_count))
        cmd = cmd.replace("${end_percent}", str(float(num + 1) / run_count))

        print("当前要执行的命令：" + cmd)

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
        print("开始日期参数不是合法的日期: " + args[1])
        os._exit(0)

    if not valid_date(args[2]):
        print("结束日期参数不是合法的日期: " + args[2])
        os._exit(0)
    # 开始日期
    start_date = datetime.datetime.strptime(args[1], '%Y-%m-%d')
    # 结束日期
    end_date = datetime.datetime.strptime(args[2], '%Y-%m-%d')

    if start_date < end_date:
        print("开始日期必须大于结束日期,开始日期为：" + start_date.__str__() + "; 结束日期为： " + end_date.__str__())
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
        sys_start_time = datetime.datetime.now()
        cur_date = start_date.strftime('%Y-%m-%d')

        # 索引FTP的数据
        run_shell_by_day(template_ftp_cmd, cur_date, pwd_path)
        # 索引聊天的数据
        run_shell_by_day(template_imchat_cmd, cur_date, pwd_path)

        # 索引HTTP的数据
        run_shell_by_day_percent(template_http_cmd, cur_date, 4, pwd_path)

        sys_end_time = datetime.datetime.now()

        print("<------------------ " + cur_date + "的数据索引完毕,时间：" + str(sys_end_time - sys_start_time) + " ------------------>")
        # 日期减1
        start_date = start_date + datetime.timedelta(days=-1)


if __name__ == "__main__":
    # 开始日期
    start_date_param = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
    # 结束日期
    end_date_param = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')

    """
    逻辑：
    历史的和实现的一起导入
    1点的时候执行实时的数据
    其他时间段执行历史的数据，如果历史的数据索引完了则程序休眠1小时
    """
    while True:
        cur_time = datetime.datetime.now()
        hour = cur_time.hour
        # 导入实时的数据
        if hour == 1:
            yesterday = cur_time + datetime.timedelta(days=-1)
            yest_date = datetime.datetime.strftime(yesterday, "%Y-%m-%d")
            print("start_date = " + yest_date)
            param_list = ['startup-daily.py', yest_date, yest_date]
            main(param_list)
        # 导入历史数据或休眠
        else:
            # 索引历史数据
            if start_date_param >= end_date_param:
                cur_date_param = datetime.datetime.strftime(start_date_param, "%Y-%m-%d")
                param_list = ['startup-daily.py', cur_date_param, cur_date_param]
                main(param_list)
                start_date_param = start_date_param + datetime.timedelta(days=-1)
            # 如果历史数据已经索引完毕则程序休眠一个小时
            else:
                time.sleep(3600)


