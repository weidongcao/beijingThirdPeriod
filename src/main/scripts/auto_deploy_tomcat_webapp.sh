#! /bin/bash

# 脚本部署步骤
# 需要修改4个变量
# tomcat_map: 需要部署的Web应用压缩包及其对应的Tomcat及其端口号,
# ocnfig_array: 需要替换的配置文件,脚本会去找旧版本里的配置文件来替换当前的,如果旧版本的不存在,则不会替换,这个时候就需要你自己手动修改了
# app_dir: Web应用压缩包存放的目录
# home_dir: Tomcat所在的目录
# app_dir与home_dir不能是同一个目录
# 修改好这个4个变量就可以执行了
#
# 脚本说明:
# 脚本主要用于部署Tomcat应用程序
# 脚本可以同时部署多个Tomcat Web项目
# 所有Web项目的压缩包需要放在指定的目录下,这个目录由app_dir变量控制
# 所有要部署的Tomcat需要统一放在同一目录下, 这个目录由home_dir变量控制
#
# 支持压缩包类型为tar.bz2, tar.gz, war, zip
# 需要修改的配置文件请添加到config_array数组里,
# 添加进去后脚本会到Tomcat原来部署的应用的相应的目录下去找
#
# 不同Tomcat的名称为"Tomcat的名字-任务名称",
# 如Tomcat的名称为apache-tomcat-7.0.79,
# 任务(task)的名称为压缩包不带后缀(如压缩包 名称为case.tar.bz2则任务名称为case)
# 那么Tomcat的全称就是apache-tomcat-7.0.79-case
#
# 关于lib依赖包:脚本肝去解压后的Web应用的WEB-INF目录下找lib目录,如果lib目录不存在则复制旧的Web应用的lib依赖包
#
# 部署应用分为以下几步(目录跳转不算)：
#   1. 解压压缩包
#   2. 复制lib依赖包(可选由if_copy_rely_lib变量控制)
#   3. 替换配置文件
#   4. 停止应用程序服务
#   5. 将解压后的应用程序部署到相应的Tomcat下
#   6. 启动Tomcat

#########################################执行脚本修改以下变量#############################################
# 应用程序部署的Tomcat
declare -A tomcat_map=(["bigdata.tar.bz2"]="apache-tomcat-7.0.79:8080" ["case.tar.bz2"]="apache-tomcat-8.5.16:8080" ["isecbiz.tar.bz2"]="apache-tomcat-8.5.16:8080")
# 需要修改的应用程序的配置文件
config_array=("jdbc.properties" "remoting.properties" "sysconfig.properties")

# 应用压缩包放置的目录
app_dir="/app/`date -d '1 days ago' "+%Y%m%d"`"
# Java Web应用部署的Tomcat所在的目录
home_dir="/home/java"
#########################################执行脚本修改以上变量#############################################

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
for compress_file_name in ${!tomcat_map[@]}
do
    # 应用程序压缩包名称(不包含后缀),这里以任务(task)命名
    task=${compress_file_name%%.*}
    # 应用程序压缩判刑
    compress_type=${compress_file_name#*.}

    # 当前应用所在的Tomcat名称(前缀)及端口号
    tomcat_and_port=${tomcat_map["${compress_file_name}"]}
    # Tomcat的名称
    tomcat_name=${tomcat_and_port%:*}-${task}
    # Tomcat端口号
    tomcat_port=${tomcat_and_port#*:}

    # Tomcat应用程序部署的WEB-INF目录
    deploy_web_inf_dir="${home_dir}/${tomcat_name}/webapps/${task}/WEB-INF"

    logger "当前任务类型: ${task}"
    logger "应用程序的压缩包名称: ${compress_file_name}"
    logger "应用程序的压缩包类型: ${compress_type}"
    logger "当前应用程序要部署的Tomcat: ${tomcat_name}"
    logger "Tomcat端口号: ${tomcat_port}"
    logger "Tomcat应用程序部署的WEB-INF目录: ${deploy_web_inf_dir}"
    echo ""

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
        rm -rf ${task}
        logger "解压要生成的目录已经存在,先删除这个目录: ${task}"
    fi

    # 第二步解压
    if [[ "${compress_type}" == "tar.bz2" ]] || [[ "${compress_type}" == "tar.gz" ]];then   # 解压tar.bz2, tar.gz包
        tar -zxf ${compress_file_name}
        logger "命令执行完成: tar -zxf ${compress_file_name}"
    elif [[ "${compress_type}" == "war" ]] || [[ "${compress_type}" == "zip" ]];then     # 解压war, zip包
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
    if [ ! -d "lib" ]; then
        logger "复制lib依赖包,命令: cp -R ${deploy_web_inf_dir}/lib ./"
        cp -R ${deploy_web_inf_dir}/lib ./
        logger "${task}没有lib依赖包, 复制原来部署的应用程序的lib依赖包完成"
    else
        logger "${task}已有lib依赖包, 不需要复制原来应用程序下的依赖包"
    fi

    # 第五步: 跳转到刚刚解压的应用的WEB-INF/classes目录下
    cd ${app_dir}/${task}/WEB-INF/classes
    logger "切换路径: $(cd `dirname $0`; pwd)"

    # 第六步: 删除刚刚解压的应用的配置文件并将应用部署目录下的配置文件复制过来
    # 主要针对三个配置文件jdbc.properties remoting.properties sysconfig.properties
    for file in ${config_array[@]}
    do
        # 判断配置配置文件是否存在
        if [ -f "${deploy_web_inf_dir}/classes/${file}" ];then     # 如果配置文件在WEB-INF/classes目录下
            rm -rf ${file}
            logger "已删除${task}应用的配置文件:${file}"

            cp ${deploy_web_inf_dir}/classes/${file} .
            logger "已将原来应用的配置文件 ${file} 复制给新的应用"
        elif [ -f "${deploy_web_inf_dir}/classes/config/${file}" ];then   # 如果配置文件在WEB-INF/classes/config目录下
            rm -rf config/${file}
            logger "已删除${task}应用的配置文件:${file}"

            cp ${deploy_web_inf_dir}/classes/config/${file} config/
            logger "已将原来应用的配置文件 ${file} 复制给新的应用"
        else
            logger "${task} 没有 ${file} 这个配置文件"
        fi
    done

    # 停止Tomcat
    echo ""
    logger "开始关闭${tomcat_name} ..."
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
        logger "移动配置好的Web应用 ${task} 到 ${tomcat_name} 完成"

        # 启动Tomcat
        logger "启动Tomcat,命令: sh bin/startup.sh"
        sh bin/startup.sh

        # 判断Tomcat是否启动成功
        if [[ $? == 0 ]];then
            logger "${tomcat_name}启动完成"
        else
            logger "Tomcat启动失败,即将退出"
            exit 1
        fi
    fi
done
logger "没有需要部署的应用程序了"
logger "所有应用程序部署完毕"

