#!/usr/bin/python
# coding=utf-8

import datetime
import subprocess
import sys
import time
import os

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
def exec_command_shell(full_command, pwd_path):
    try:
        result_cmd = subprocess.Popen(full_command, cwd=pwd_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, shell=True)
        # 等等命令行运行完
        result_cmd.wait()

        # 获取命令行输出
        stdout = result_cmd.stdout.read()
        # print("stdout = " + str(stdout))

        # 获取命令行异常
        stderr = result_cmd.stderr.read()
        # print("stderr = " + str(stderr))

        # 获取shell 命令返回值,如果正常执行会返回0, 执行异常返回其他值
        return_code = result_cmd.returncode
        # print("return_code = " + str(return_code))

        # 获取命令运行进程号
        pid = result_cmd.pid

        result_dict = {"stdout": stdout, "stderr": stderr, "return_code": return_code, "pid": pid}
        return result_dict
    except Exception as e:
        print(e.message)
        return False


if __name__ == "__main__":

    template_cmd = "java -classpath BeiJingThirdPeriod.jar com.rainsoft.solr.OracleDataCreateSolrIndex {start_date} {end_date} " \
          "once "
    # template_cmd = "python TestPython.py {start_date} {end_date}"
    # 开始日期
    start_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
    # 结束日期
    end_date = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')

    pwd_path = os.getcwd()
    print("当前根目录为：" + pwd_path)

    while start_date >= end_date:
        cmd = template_cmd.replace("{start_date}", start_date.strftime('%Y-%m-%d'))
        cmd = cmd.replace("{end_date}", start_date.strftime('%Y-%m-%d'))
        print("执行命令：" + cmd)
        result_dict = exec_command_shell(cmd, pwd_path)
        print(start_date.strftime('%Y-%m-%d'))
        if result_dict["return_code"] != 0:
            print("程序执行失败,程序即将退出")
            break
        start_date = start_date + datetime.timedelta(days=-1)

        # 休眠5秒, 等待内存清理完毕
        time.sleep(5)

