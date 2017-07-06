#!/usr/bin/python
# coding=utf-8

import datetime
import subprocess
import sys
import time
import os

template_ftp_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.FtpOracleDataCreateSolrIndex ${cur_date}"
template_http_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.HttpOracleDataCreateSolrIndex ${cur_date} ${start_percent} ${end_percent}"
template_imchat_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.ImchatOracleDataCreateSolrIndex ${cur_date}"

template_test_cmd = "python TestPython.py {start_date} {end_date}"

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


def exec_command_shell(full_command, cwd_path):
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
    result_dict = exec_command_shell(cmd, pwd_path)
    if result_dict["return_code"] != 0:
        print("程序执行失败,程序即将退出")
        os._exit(0)
    # 休眠, 等待内存清理完毕
    time.sleep(5)

def valid_date(str):
    """判断是否是一个有效的日期字符串"""
    try:
        time.strptime(str, "%Y-%m-%d")
        return True
    except:
        return False


def main(args):

    """参数校验"""
    if valid_date(args[1]) == False:
        print("开始日期参数不是合法的日期: " + args[1])
        return

    if valid_date(args[2]) == False:
        print("结束日期参数不是合法的日期: " + args[2])
        return

    # 开始日期
    start_date = datetime.datetime.strptime(args[1], '%Y-%m-%d')
    # 结束日期
    end_date = datetime.datetime.strptime(args[2], '%Y-%m-%d')

    if(start_date < end_date):
        print("开始日期必须大于结束日期,开始日期为：" + start_date.__str__() + "; 结束日期为： " + end_date.__str__())
        return

    pwd_path = os.getcwd()
    print("当前根目录为：" + pwd_path)

    """指执行命令"""
    while start_date >= end_date:
        cur_date = start_date.strftime('%Y-%m-%d')

        # 索引FTP的数据
        ftp_cmd = template_ftp_cmd.replace("${cur_date}", cur_date)
        print("当前要执行的命令：" + ftp_cmd)
        run_shell(ftp_cmd, pwd_path)

        # 索引聊天的数据
        imchat_cmd = template_imchat_cmd.replace("${cur_date}", cur_date)
        print("当前要执行的命令：" + imchat_cmd)
        run_shell(imchat_cmd, pwd_path)

        http_run_count = 4
        # 休眠5秒, 等待内存清理完毕
        for num in range(http_run_count):
            http_cmd = template_http_cmd.replace("${cur_date}", cur_date)
            http_cmd = http_cmd.replace("${start_percent}", str(float(num) / http_run_count))
            http_cmd = http_cmd.replace("${end_percent}", str(float(num + 1) / http_run_count))

            print("http_cmd = " + http_cmd)
            print("当前要执行的命令：" + http_cmd)

            run_shell(http_cmd, pwd_path)

        print("<--------------------------------- " + cur_date + "的数据索引完毕 ------------------------------------------>")

        start_date = start_date + datetime.timedelta(days=-1)

    print("<--------------------------------- 数据索引完毕程序退出 ------------------------------------------>")

if __name__ == "__main__":
    main(sys.argv)


