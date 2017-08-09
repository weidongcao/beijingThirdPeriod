#!/usr/bin/python
# coding=utf-8

import datetime
import subprocess
import sys
import threading
import time
import os

"""
有三个目录：
data:程序把oracle的数据读出写入本地的目录
pool:写入完毕后会把文件移到这个目录下
work:程序先从pool里把数据移到此目录下，然后对此目录下的数据上传到hdfs
"""

template_load_oracle = """
java -classpath BeiJingThirdPeriod.jar com.rainsoft.oracle.LoadOracleData ${table_name} '${start_time}' '${end_time}'
"""
local_base_path = "/opt/modules/BeiJingThirdPeriod/oracle_workspace"
hdfs_base_path = "/tmp/oracle"

template_oracle_table_name = "reg_content_${task_type}"

# 上传数据到HDFS，先把数据从pool目录移动到work目录下
template_mv_cmd = """
mv ${local_base_path}/pool/${table_name}/${table_name}_data_*.tsv ${local_base_path}/work/${table_name}
"""
# 上传数据到HDFS的命令
template_upload_hdfs = """
hdfs dfs -put ${local_base_path}/work/${table_name} ${hdfs_base_path}/${table_name}
"""
# 数据上传完后删除本地数据
template_delete_local = """
rm -f ${local_base_path}/work/${table_name}/${table_name}_data_*.tsv
"""

template_import_shell = """
spark-submit \
--master yarn-client \
--executor-memory 64g \
--executor-cores 16 \
--num-executors 4 \
--class com.rainsoft.hbase.SparkExportToHBase \
/opt/modules/BeiJingThirdPeriod/BeiJingThirdPeriod.jar ${task_type} ${hdfs_base_path}/${table_name}
"""


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
            print process.stdout.readline(),
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
    suffex_info = "[{0} INFO]  ".format(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"))

    print(suffex_info + content)


def run_shell(cmd, pwd_path):
    """
    执行Linux Shell命令，如果执行失败退出程序
    :param cmd: Shell命令
    :param pwd_path: 执行Shell命令时所在的目录
    :return:
    """
    format_print("当前要执行的命令： " + cmd)
    # result_dict = exec_command_shell(cmd, pwd_path)
    result_dict = {"return_code": 0}
    if result_dict["return_code"] != 0:
        print("命令执行失败,程序即将退出")
        os._exit(0)
    # 休眠, 等待内存清理完毕
    format_print("命令执行完毕")
    time.sleep(1)


def run_upload_spark(task_type, table_name, local_base_path, hdfs_base_path):


    upload_count = 0
    pwd_path = os.getcwd()
    table_name = template_oracle_table_name.replace("${task_type}", task_type)

    #数据文件移动到工作目录的命令
    local_mv_cmd = template_mv_cmd.replace("${local_base_path}", local_base_path) \
        .replace("${table_name}", table_name)

    # 数据文件上传到HDFS的命令
    upload_hdfs_cmd = template_upload_hdfs.replace("${table_name}", table_name) \
        .replace("${local_base_path}", local_base_path) \
        .replace("${hdfs_base_path}", hdfs_base_path)

    # Spark处理HDFS上的数据文件的命令
    import_shell = template_import_shell.replace("${task_type}", task_type) \
        .replace("${hdfs_base_path}", hdfs_base_path) \
        .replace("${table_name}", table_name)

    # 删除本地数据文件的命令
    delete_local_cmd = template_delete_local.replace("${local_base_path}", local_base_path) \
        .replace("${table_name}", table_name)

    while True:
        # 上传到HDFS的文件达
        if upload_count < 200:
            # 将数据文件移动到工作目录
            run_shell(local_mv_cmd, pwd_path)
            # 上传的HDFS文件没有达到指定数目, 继续上传
            run_shell(upload_hdfs_cmd, pwd_path)
            # 删除本地数据文件
            run_shell(delete_local_cmd, pwd_path)
            format_print(table_name + " 数据上传到HDFS完毕...")
            upload_count += 1
            time.sleep(10)
        else:
            # 达到指定数据，调用Spark对其进行处理
            run_shell(import_shell, pwd_path)
            format_print("HDFS数据处理完毕...")
            # 处理完删除HDFS目录下的数据文件
            format_print(table_name + " HDFS数据删除完毕...")
            # 重新统计上传文件个数
            upload_count = 0
            time.sleep(1)


def run(args):
    table_name = args[0]

    main_start_time = args[1]
    main_end_time = args[2]

    # 根目录
    pwd_path = os.getcwd()
    # 程序开始时间
    sys_start_time = datetime.datetime.now()

    load_oracle_cmd = template_load_oracle.replace("${table_name}", table_name) \
        .replace("${start_time}", main_start_time) \
        .replace("${end_time}", main_end_time)
    run_shell(load_oracle_cmd, pwd_path)

    # 程序结束时间
    sys_end_time = datetime.datetime.now()

    # 导入一天的数据程序消耗的时间
    format_print("<--------------- " + main_start_time + " ~ " + main_end_time + "的数据索引完毕,耗时：" +
                 str(sys_end_time - sys_start_time).split(".")[0] + " --------------->")


def main():
    # 每次处理多久的数据单位分钟，1440为一天
    offset = 360
    task_type = "http"
    # 从record.txt读取开始迁移的日期,格式为:YYYY-MM-dd HH:MM:SS ,record.txt不能为空,
    record_file = open('createIndexRecord/import_oracle_to_hbase_record.txt')

    try:
        start_time_param = record_file.read()
    except Exception as e:
        print(e)
    finally:
        record_file.close()
    table_name = template_oracle_table_name.replace("${task_type}", task_type)

    threading.Thread(target=run_upload_spark, args=(task_type, table_name, local_base_path, hdfs_base_path)).start()

    start_time = datetime.datetime.strptime(start_time_param, "%Y-%m-%d %H:%M:%S")

    # 数据库保留1年的数据
    data_store_days = 365
    cur_time = datetime.datetime.now()
    flat = (cur_time - start_time).days < data_store_days

    """
    逻辑：
    历史的和实现的一起导入
    1点的时候执行实时的数据
    其他时间段执行历史的数据，如果历史的数据索引完了则程序休眠1小时
    """
    while flat:
        start_time_param = datetime.datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        end_time = start_time - datetime.timedelta(minutes=offset)
        end_time_param = datetime.datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        table_name = template_oracle_table_name.replace("${task_type}", task_type)
        param_list = [table_name, end_time_param, start_time_param]
        run(param_list)

        record_file = open('createIndexRecord/import_oracle_to_hbase_record.txt', 'w')
        record_file.write(end_time_param)
        record_file.close()

        start_time = end_time
        days = (cur_time - start_time).days
        flat = days < data_store_days
        time.sleep(8)
    format_print("一年数据迁移完毕...")

if __name__ == "__main__":
    main()
