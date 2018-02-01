# 管综项目说明

----------
## 使用说明 ##
 - 部署项目时根据情况看document目录下的文档

## 压缩包文件说明 ##

 - lib文件夹
项目依赖的jar包

 - config文件夹
项目的配置文件,单独查看用,应用执行的时候与这个没有关系

 - document文件夹
项目说明文档

 - BeiJingThirdPeriod.jar文件
应用jar包

 - oracle_history_export.sh脚本
跑历史任务

 - oracle_real_time_export.sh脚本
跑实时任务,从Oracle取数据

 - real_time_import_bcp.sh脚本
跑实时任务，从Bcp文件取数据

## 设计说明 ##
应用的核心配置文件有两个：
第一个：application.properties
配置应用的常用配置
第二个：column.json
这个配置文件用来存放字段名相关的配置，具体如下：
 1. bcp文件的字段结构
 2. bcp需要过滤的字段Oracle的字段结构
 3. Oracle表的字段
 4. Solr的字段类型
 5. Solr不需要写入的字段
 应用处理数据的时候是从column.json读取字段名,数据存放在JavaRDD<Row>里，一个Row就是一条数据，数据全部是String类型。数据通过下标与column.json配置文件的字段名一一对应，通过key获取到字段的类型
 Java处理数据一个很常用的做法是把数据封装成实体，然后对实体类进行操作这种方式对于JavaWeb项目非常实用，每一个实体保存的都有字段名、字段类型、字段值，操作非常方便。
 但是在大数据里这种方式是非常占内存的，JavaWeb一般只操作几十几百个实体类能上千就已经了不得了，
但是大数据处理数据很随意就上亿了，然后光字段名字段类型就占了一半以上的内存，不可取。




