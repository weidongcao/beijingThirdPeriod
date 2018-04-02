#! /bin/bash

# 脚本说明
# 脚本主要用于部署Tomcat应用程序
# 有三个Tomcat的应用程序:
# bigdata.tar.bz2
# case.tar.bz2
# isecbiz.tar.bz2
# 支持压缩包类型为tar.bz2, tar.gz, war, zip,由变量compress_type控制
# 看情况是否需要复制原来应用程序的依赖包,由if_copy_rely_lib变量控制
# 不同Tomcat的名称为"Tomcat的名字-应用名称",如Tomcat的名称为apache-tomcat-7.0.79, 应用的名称为bigdata
# 那么Tomcat的全称就是apache-tomcat-7.0.79-bigdata
#
# 部署应用分为以下几步(目录跳转不算)：
#   1. 解压压缩包
#   2. 复制lib依赖包(可选由if_copy_rely_lib变量控制)
#   3. 修改配置文件(删除原来的解压后的配置文件，复制原来应用程序的配置文件)
#   4. 停止应用程序服务
#   5. 将解压后的应用程序部署到相应的Tomcat下
#   6. 启动Tomcat

# 应用程序部署的Tomcat
declare -A tomcat_map=(["bigdata"]="apache-tomcat-7.0.79" ["case"]="apache-tomcat-8.5.16" ["isecbiz"]="apache-tomcat-8.5.16")
# 应用程序所在Tomcat访问时的端口号
declare -A port_map=(["bigdata"]="" ["case"]="" ["isecbiz"]="")
# 需要修改的应用程序的配置文件
config_array = ("jdbc" "remoting" "sysconfig")

# 应用压缩包放置的目录
app_dir="/app/`date -d '1 days ago' "+%Y%m%d"`"
# Java Web应用部署的Tomcat所在的目录
home_dir="/home/java"
# 压缩包类型
compress_type=".tar.bz2"
# 是否需要复制依赖包
if_copy_rely_lib="true"

# 测试用
# declare -A tomcat_map=(["yisou"]="tomcat")
# declare -A port_map=(["yisou"]="8090")
# config_array=("solr")
#
# app_dir="/opt/software"
# home_dir="/solrCloud"
# compress_type=".war"
# if_copy_rely_lib="true"

# 打印日志
function logger() {
    # 写入日志文件
    echo "[`date "+%Y-%m-%d %H:%M:%S"`  INFO] ${1}"  ## >> logs/bcp_to_hbase_solr-`date "+%Y%m%d"`.log
}

# 执行命令并将命令的输出写入日志文件
# 如果不需要写入日志文件可以直接执行命令，不调这个方法
# 返回命令执行结果状态
# 第一个参数：命令
# 第二个参数：命令的参数
function run_cmd(){
    logger "要执行的命令：${1} ${2}"
    ## 执行命令并将输出结果写入日志文件
    ${1} ${2}    >> logs/update-`date "+%Y%m%d"`.log
    return ${cmdRunResultStatus}
}

## 杀掉给出的进程号可能有多个,故for遍历所有
function killProcess() {
	cmdRunResultStatus=-1
    for port in ${1}
    do
        # 判断进程是否是合法的数字,如果不是的话跳过
        if [ "${port}" -gt 0 ] 2>/dev/null ;then
            logger "要杀掉的进程的端口号为${port}"
        else
            logger "不存在此端口号的进程,端口号为: ${port}"
            continue
        fi

        # 杀掉进程
    	while [ ${cmdRunResultStatus} -ne 0 ];
    	do
    		# 查看进程是否存在,存在的话获取进程PID
    		# 这个命令的意思是:
    		# netstat -apn | grep ${1}    根据关键字${1},列出所有连接的网络信息
    		# grep "LISTEN"               只保留状态为LISTEN的信息
    		# awk '{print $7}'            打印网络连接信息的第七列,第七列是"端口号/进程名称",不同的列之间以空白符分隔(默认)
    		# awk -F '/' '{print $1}'      第七列信息以/分隔,打印第一列,第一列是进程号(pid),第二列是进程名
    		# sort -u     对以上获取到的PID进行去重,很多时候以上命令返回的不止一行，但是都是相同的PID
    		pid=`netstat -apn | grep ${port} | grep "LISTEN"| awk '{print $7}' | awk -F '/' '{print $1}' | sort -u`
    		if [ -n "${pid}" ]; then      # 不存在此进程
                # 杀掉进程,如果杀不死,循环杀,直到杀死
                while [[ true ]]; do
        			kill -9 ${pid}
                    # 获取命令执行返回结果(执行成功返回0)
        			cmdRunResultStatus=$?
                    # 如果命令执行结果正常,跳出循环
                    if [[ ${cmdRunResultStatus} == 0 ]]; then
        				logger "进程被杀掉, 进程号:${pid}, 进程端口号:${1}"
                        break
                    fi
                done

                # 释放资源
				logger "等待进程资源释放..."
				while [[ true ]];
				do
                    # 等待3秒再执行后面的命令,以防止有缓存
					sleep 2s

                    # 再次判断进程是否存在
					old_pid=`netstat -apn | grep ${port} | grep -v grep | grep -v $$ | awk '{print $1}'`
					if [ -z "${old_pid}"]; then
						logger "进程资源释放完成."
                        sleep 3s
						break
					fi
				done
    		else      # 存在,杀掉此进程
    			logger "进程没有启动: ${1}"
                # 如果进程没有启动跳出循环并结束
    			if [ ${cmdRunResultStatus} -eq -1 ]; then
    				cmdRunResultStatus=0
    			fi
    			break;
    		fi
    	done
    done
	return ${cmdRunResultStatus}
}

# 循环部署Tomcat应用程序
for task in ${!tomcat_map[@]}
do
    # 当前应用所在的Tomcat名称
    tomcat_name=${tomcat_map["${task}"]}-${task}
    # 应用程序压缩包名称
    compress_file_name="${task}${compress_type}"
    # Tomcat端口号
    tomcat_port=${port_map["${task}"]}
    # Tomcat应用程序部署的WEB-INF目录
    deploy_web_inf_dir="${home_dir}/${tomcat_name}/webapps/${task}/WEB-INF"

    logger "应用程序的压缩包名称: ${compress_file_name}"
    logger "当前应用程序要部署的Tomcat: ${tomcat_name}"
    logger "Tomcat端口号: ${tomcat_port}"
    logger "Tomcat应用程序部署的WEB-INF目录: ${deploy_web_inf_dir}"

    # 第一步跳转到app根目录如/app/20180331
    cd ${app_dir}
    logger "切换路径: $(cd `dirname $0`; pwd)"

    # 先判断应用程序的压缩包是否存在
    # 如果不存在的话不进行部署,并跳过后面的操作直接部署下一个
    if [ ! -f "${compress_file_name}" ];then
        logger "要处理的应用程序压缩包不存在,跳过直接部署下一个"
        continue
    fi

    # 判断解压要生成的目录是否存在，如果存在的话删除
    if [ -d "${task}" ];then
        logger "解压要生成的目录已经存在,先删除这个目录: ${task}"
        rm -rf ${task}
        logger "解压要生成的目录已删除"
    fi

    # 第二步解压
    if [[ "${compress_type}" == ".tar.bz2" ]] || [[ "${compress_type}" == ".tar.gz" ]];then   # 解压.tar.bz2, .tar.gz包
        tar -zxf ${compress_file_name}
        logger "命令执行完成: tar -zxf ${compress_file_name}"
    elif [[ "${compress_type}" == ".war" ]] || [[ "${compress_type}" == ".zip" ]];then     # 解压.war, .zip包
        logger "命令执行完成: unzip -q ${compress_file_name} -d ${task}"
        unzip -q ${compress_file_name} -d ${task}
    fi

    # unzip ${compress_file_name} -d ${task}
    logger "${compress_file_name}解压完成"

    # 第三步: 跳转到解压后的WEB-INF目录下
    cd ${app_dir}/${task}/WEB-INF
    logger "切换路径: $(cd `dirname $0`; pwd)"

    # 第四步: 复制应用部署目录下所有的lib包到刚刚解压的应用下
    # 在自己电脑上测试的时候发现自己打的war包不需要复制原来应用程序的依赖包,
    # 而雅妮的应用程序包是从成都传过来的,打包的时候没有把依赖包打进行
    # 帮进行以下判断,是否复制依赖包由变量: if_copy_rely_lib控制
    if [[ "true" == "${if_copy_rely_lib}" ]]; then
        logger "复制lib依赖包,命令: cp -R ${deploy_web_inf_dir}/lib ./"
        cp -R ${deploy_web_inf_dir}/lib ./
        logger "${task}复制原来部署的应用程序的lib依赖包完成"
    else
        logger "${task}不需要复制原来应用程序中WEB-INF/lib下的依赖包"
    fi

    # 第五步: 跳转到刚刚解压的应用的WEB-INF/classes目录下
    cd ${app_dir}/${task}/WEB-INF/classes
    logger "切换路径: $(cd `dirname $0`; pwd)"

    # 第六步: 删除刚刚解压的应用的配置文件并将应用部署目录下的配置文件复制过来
    # 主要针对三个配置文件jdbc.properties remoting.properties sysconfig.properties
    for conf in ${config_array[@]}
    do
        file="${conf}.properties"
        # 判断配置配置文件是否存在
        if [ -f "${deploy_web_inf_dir}/classes/${file}" ];then     # 如果配置文件在WEB-INF/classes目录下
            rm -rf ${file}
            logger "已删除${task}应用的配置文件:${file}"
            cp ${deploy_web_inf_dir}/classes/${file} .
            logger "已将原来应用的配置文件 ${file} 复制给新的应用"
        elif [ -f "${deploy_web_inf_dir}/classes/config/${file}" ];then   # 如果配置文件在WEB-INF/classes/config目录下
            rm -rf config/${file}
            cp ${deploy_web_inf_dir}/classes/config/${file} config/
            logger "已将原来应用的配置文件 ${file} 复制给新的应用"
        else
            logger "${task} 没有找到 ${file} 这个配置文件"
        fi
    done

    # 停止Tomcat
    killProcess ${tomcat_port}

    # 如果停止成功(上面的命令返回值为0)则继续部署应用
    if [ $? -eq 0 ];then
        #跳转到当前应用所在的Tomcat根目录下
        cd ${home_dir}/${tomcat_name}

        # 判断原来的应用是否存在，如果存在的话删除原来部署的应用
        if [ -d "webapps/${task}" ];then
            rm -rf webapps/${task}
            logger "删除原来的应用程序完成: ${task}"
        else
            logger "Tomcat没有部署应用 ${task}, 不需要删除"
        fi

        # 把配置好的应用复制到Tomcat的webapps目录下
        cp -R ${app_dir}/${task} webapps/
        logger "移动配置好的应用程序${task}到Tomcat完成"

        # 启动Tomcat
        sh bin/startup.sh
        # 判断Tomcat是否启动成功
        if [[ $? == 0 ]];then
            logger "${task}应用的Tomcat启动完成"
        else
            logger "Tomcat启动失败,即将退出"
            exit 1
        fi
    fi
done
logger "没有需要部署的应用程序了"
logger "所有应用程序部署完毕"
