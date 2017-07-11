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
        process = subprocess.Popen(full_command, cwd=pwd_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, shell=True)
        while True:
            print process.stdout.readline()
            if process.poll() is not None:
                break

        process.wait()

        stdout = process.stdout.read()
        #print("stdout = " + str(stdout))
        stderr = process.stderr.read()
        return_code = process.returncode
        pid = process.pid

        result_dict = {"stdout": stdout, "stderr": stderr, "return_code": return_code, "pid": pid}
        return result_dict
    except Exception as e:
        print(e.message)
        return False


# 执行一个Shell命令,执行命令写入日志,如果执行失败尝试3次，如果3次都失败退出程序
def run_command(cmd, log_path):
    # 如果程序执行失败，写入日志
    cmd_with_log = cmd + " >> " + log_path

    # 尝试3次，如果3次都执行失败退出程序
    try_count = 0
    while True:
        exec_command_shell("echo `date '+%Y-%m-%d %H:%M:%S '`'开始执行命令：" + cmd + "' >> " + log_path, os.getcwd())
        # 执行命令
        result_dict = exec_command_shell(cmd_with_log, os.getcwd())

        # 判断程序执行结果
        if result_dict["return_code"] != 0:
            if try_count < 3:
                try_count += 1
            else:
                exec_command_shell("echo `date '+%Y-%m-%d %H:%M:%S '`'命令执行失败 ' >> " + log_path, os.getcwd())
                os._exit(0)
        else:
            exec_command_shell("echo `date '+%Y-%m-%d %H:%M:%S '`'命令执行完毕： ' >> " + log_path, os.getcwd())
            break

    # 休眠,等等内存清理完毕
    time.sleep(5)


# 将BBS数据写入HBase
template_cmd_importtsv_bbs = "/opt/hbase/importtsv-bbs.sh"
# 将BBS_TMP数据导入Solr
template_cmd_spark_bbs = "spark-submit --master yarn-client --class main.scala.tmpJob.Bbs /solrCloud/SparkJob/SparkJob.jar"
# 清除BBs_EMP 中的数据
template_cmd_hbase_del_bbs = "/opt/bcp/sh/del_hbase_sh/del_hbase_tmp_bbs.sh"

# 将EMAIL数据写入HBase
template_cmd_importtsv_email = "/opt/hbase/importtsv-email.sh"
# 将EMAIL_TMP数据导入Solr
template_cmd_spark_email = "spark-submit --master yarn-client --class main.scala.tmpJob.Email /solrCloud/SparkJob/SparkJob.jar"
# 清除EMAIL_EMP 中的数据
template_cmd_hbase_del_email = "/opt/bcp/sh/del_hbase_sh/del_hbase_tmp_email.sh"

# 将FTP数据写入HBase
template_cmd_importtsv_ftp = "/opt/hbase/importtsv-ftp.sh"
# 将FTP_TMP数据导入Solr
template_cmd_spark_ftp = "spark-submit --master yarn-client --class main.scala.tmpJob.Ftp /solrCloud/SparkJob/SparkJob.jar"
# 清除FTP_EMP 中的数据
template_cmd_hbase_del_ftp = "/opt/bcp/sh/del_hbase_sh/del_hbase_tmp_ftp.sh"

# 将HTTP数据写入HBase
template_cmd_ = "/opt/hbase/importtsv-http.sh"
# 将HTTP_TMP数据导入Solr
template_cmd_spark_ = "spark-submit --master yarn-client --class main.scala.tmpJob.Http /solrCloud/SparkJob/SparkJob.jar"
# 清除HTTP_EMP 中的数据
template_cmd_hbase_del_ = "/opt/bcp/sh/del_hbase_sh/del_hbase_tmp_http.sh"

# 将IMCHAT数据写入HBase
template_cmd_importtsv_imchat = "/opt/hbase/importtsv-imchat.sh"
# 将IMCHAT_TMP数据导入Solr
template_cmd_spark_imchat = "spark-submit --master yarn-client --class main.scala.tmpJob.Imchat /solrCloud/SparkJob/SparkJob.jar"
# 清除IMCHAT_EMP 中的数据
template_cmd_hbase_del_imchat = "/opt/bcp/sh/del_hbase_sh/del_hbase_tmp_imchat.sh"

# 将SEARCH数据写入HBase
template_cmd_importtsv_search = "/opt/hbase/importtsv-search.sh"
# 将SEARCH_TMP数据导入Solr
template_cmd_spark_search = "spark-submit --master yarn-client --class main.scala.tmpJob.Search /solrCloud/SparkJob/SparkJob.jar"
# 清除SEARCH_EMP 中的数据
template_cmd_hbase_del_search = "/opt/bcp/sh/del_hbase_sh/del_hbase_tmp_search.sh"

# 将WEIBO数据写入HBase
template_cmd_importtsv_weibo = "/opt/hbase/importtsv-weibo.sh"
# 将WEIBO_TMP数据导入Solr
template_cmd_spark_weibo = "spark-submit --master yarn-client --class main.scala.tmpJob.Weibo /solrCloud/SparkJob/SparkJob.jar"
# 清除WEIBO_EMP 中的数据
template_cmd_hbase_del_weibo = "/opt/bcp/sh/del_hbase_sh/del_hbase_tmp_weibo.sh"


def main(args):
    path = os.getcwd()
    log_path = path + "/log" + time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".log"

    while True:
        # 将BBS数据写入HBase
        run_command(template_cmd_importtsv_bbs, log_path)
        # 将BBS_TMP数据导入Solr
        run_command(template_cmd_spark_bbs, log_path)
        # 清除BBs_EMP 中的数据
        run_command(template_cmd_hbase_del_bbs, log_path)

        # 将EMAIL数据写入HBase
        run_command(template_cmd_importtsv_email, log_path)
        # 将EMAIL_TMP数据导入Solr
        run_command(template_cmd_spark_email, log_path)
        # 清除EMAIL_EMP 中的数据
        run_command(template_cmd_hbase_del_email, log_path)

        # 将FTP数据写入HBase
        run_command(template_cmd_importtsv_ftp, log_path)
        # 将FTP_TMP数据导入Solr
        run_command(template_cmd_spark_ftp, log_path)
        # 清除FTP_EMP 中的数据
        run_command(template_cmd_hbase_del_ftp, log_path)

        # 将HTTP数据写入HBase
        run_command(template_cmd_, log_path)
        # 将HTTP_TMP数据导入Solr
        run_command(template_cmd_spark_, log_path)
        # 清除HTTP_EMP 中的数据
        run_command(template_cmd_hbase_del_, log_path)

        # 将IMCHAT数据写入HBase
        run_command(template_cmd_importtsv_imchat, log_path)
        # 将IMCHAT_TMP数据导入Solr
        run_command(template_cmd_spark_imchat, log_path)
        # 清除IMCHAT_EMP 中的数据
        run_command(template_cmd_hbase_del_imchat, log_path)

        # 将SEARCH数据写入HBase
        run_command(template_cmd_importtsv_search, log_path)
        # 将SEARCH_TMP数据导入Solr
        run_command(template_cmd_spark_search, log_path)
        # 清除SEARCH_EMP 中的数据
        run_command(template_cmd_hbase_del_search, log_path)

        # 将WEIBO数据写入HBase
        run_command(template_cmd_importtsv_weibo, log_path)
        # 将WEIBO_TMP数据导入Solr
        run_command(template_cmd_spark_weibo, log_path)
        # 清除WEIBO_EMP 中的数据
        run_command(template_cmd_hbase_del_weibo, log_path)

        ## 程序休息5分钟
        time.sleep(300)

if __name__ == "__main__":
    main(sys.argv)

    # for test
    # path = os.getcwd()
    # log_path = path + "/log.log"
    # run_command("ls akdj", log_path)


