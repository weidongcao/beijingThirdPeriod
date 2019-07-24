
"""
SQL Server存储过程实现的功能:
修改指定表,指定字段的日期为指定时间段
参数说明:

@table_name 	表名
@time_column	要修改时间的日期字段
@startTime		开始时间
@endTime		结束时间
@key_col		可以唯一定位这条数据的主键,一般为表ID


例:
	exec dbo.pro_modify_time_range view_PatrolRecord,patroltime,'2019-07-18 00:00:00','2019-07-24 00:00:00',id
功能限制:
	1. 只能更新一个表的一个日期字段
	2. 表数据量不能太大
	3. 表必须有唯一主键
"""
create proc pro_modify_time_range
@table_name varchar(50),	-- 
@time_column varchar(50),
@startTime datetime,
@endTime datetime,
@key_col varchar(50)
as
	--declare @table_name varchar(50)
	--declare @time_column varchar(50)
	--declare @startTime datetime
	--declare @endTime datetime
	--declare @key_col varchar(50)
	
	declare @error int  --事务中操作的错误记录
	declare @rand_date datetime		--	指定时间段内的一个随机的时间
	declare @diffStartEndTime int	-- 指定时间段内开始时间与结束时间之间相差的秒数
	declare @startTimestamp int		-- 开始时间的秒数
	declare @record_id int			-- 一条记录中key_col对应的值
	declare @sql_select varchar(1000)	-- 查询SQL,通过这条SQL获取表中所有的主键,然后根据主键逐条更新数据
	declare @sql_update varchar(1000)	-- 更新SQL

	declare @temp_real_sql_select varchar(1000)	--查询主键SQL临时拼装
	declare @temp_real_sql_update varchar(1000)	-- 更新语句SQL临时拼装

	declare @template_sql_select varchar(1000)	--查询SQL模板
	declare @template_sql_update varchar(1000)	--更新SQL模板
	declare @real_sql_select varchar(1000)		--最后真正要执行的查询SQL
	declare @real_sql_update varchar(1000)		--最后真正要执行的更新SQL
	
	--set @table_name='view_PatrolRecord'
	--set @time_column='patroltime'
	--set @startTime='2019-07-18 00:00:00'
	--set @endTime='2019-07-24 00:00:00'
	--set @key_col='id'
	
	-- 赋值最后真正要执行的查询SQL
	set @template_sql_select='declare mycursor cursor for select ${key_col} from ${table_name};'
	-- 赋值最后真正要执行的更新SQL
	set @template_sql_update='update ${table_name} set  ${time_column} = ''${rand_date}'' where ${key_col} = ${record_id};'

	-- 赋值 指定时间段内开始时间与结束时间之间相差的秒数
	set @diffStartEndTime=datediff(S, @startTime, @endTime)
	-- 赋值 开始时间的秒数
	set @startTimestamp=datediff(S, '1970-01-01 08:00:00', @startTime)
	
	-- 赋值 查询主键SQL拼装
	set @real_sql_select = replace(@template_sql_select, '${key_col}', @key_col)
	set @real_sql_select = replace(@real_sql_select, '${table_name}', @table_name)
	
	-- 赋值 更新SQL拼装,最后只剩下需要赋值的主键值和随机日期
	set @temp_real_sql_update = replace(@template_sql_update, '${key_col}', @key_col)
	set @temp_real_sql_update = replace(@temp_real_sql_update, '${table_name}', @table_name)
	set @temp_real_sql_update = replace(@temp_real_sql_update, '${time_column}', @time_column)

	--select @real_sql_select, @temp_real_sql_update
	--申明游标
	--declare mycursor cursor for
	begin transaction
		--@real_sql_select
		exec (@real_sql_select)
		--select id from view_PatrolRecord where id < 20
		--打开游标--
		open mycursor
		--开始循环游标变量, 把查询到的主键赋给变量--
		fetch next from mycursor into @record_id
		-- 当游标为空时结束循环
		while @@FETCH_STATUS = 0 
		begin 
			-- 生成指定时间范围内的随机日期
			select @rand_date = DATEADD(S,1563379200 + rand() * 561600,'1970-01-01 08:00:00')
			
			-- 为更新语句赋值
			set @real_sql_update = replace(@temp_real_sql_update, '${rand_date}',@rand_date)
			set @real_sql_update = replace(@real_sql_update, '${record_id}',@record_id)
			
			-- 执行更新语句
			exec(@real_sql_update)
			set @error+=@@ERROR --记录有可能产生的错误号
			
			-- 获取下一条数据的主键
			fetch next from mycursor into @record_id 
		end
		close mycursor --关闭游标
		deallocate mycursor --释放游标
	--判断事务的提交或者回滚
	if(@error<>0)
	  begin
		rollback transaction
		return -1 --设置操作结果错误标识
	  end
	else
	  begin
		commit transaction
		return 1 --操作成功的标识
	  end