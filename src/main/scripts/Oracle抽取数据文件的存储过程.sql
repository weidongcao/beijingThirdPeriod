--创建路径
--create or replace directory DIR_EXTRA_PATH as '/opt/data/lyzhou';

-- 创建存储过程
-- 执行的时候需要两个参数,一个是表名，一个是截止ID
-- 程序会从指定的表从最小的ID开始抽取数据,一直抽取到截止ID
-- 存储过程逻辑说明
-- 1. 初始化参数,
--      先初始化与BCP文件名相关的固定不变的参数,如采集系统标识,结构化非结构化,生成的文件后缀等
--      再初始化经常变的参数,主要有:
--          1. 要生成的BCP文件名(v_fname)
--          2. 起始ID(v_id_start)
--          3. 偏移量(v_id_offset,一次从Oracle里抽取多少数据,默认抽取1000条，不足1000条的话就需要根据情况进行变更了)
--          4. 截止ID(v_id_end),如果指定的截止ID大于表的最大ID的话只抽取到最大ID
--          5. 查询返回结果的select语句(v_sql_select),它会把要查询的所有字段以制表符拼接起来,制表符以chr(9)表示
-- 生成文件名
-- 判断是否有数据,如果没有的话直接退出，如果有的话打开一个文件
-- 以下为循环
-- 2. 判断偏移量
-- 3. 生成查询SQL
-- 4. 判断查询SQL是否会有数据,如果没有的话不生成BCP文件，直接跳过进行下一次循环
-- 5. 从游标中获取一条查询数据,并将数据写入BCP文件
-- 6. 文件写入条数加1,判断写入文件的数据量是否超过文件默认写入条数(默认条为1000)
-- 7. 关闭旧的文件写入流
-- 8. 生成的BCP文件数加1,文件写入条数清零,生成新的文件名
-- 9. 打开新文件的读写流操作
-- 10. 起始ID加上偏移量，准备下一次循环
-- 11. 判断是否处理完成,处理完成的话关闭文件写入流并结束循环
create or replace procedure proc_extra_bcp (
       v_table_name in varchar, --表名
       v_iend in number  -- 结束ID
)
as
begin
declare
    -------------------------BCP文件相关变量---------------------------------
    -- 生成的数据文件名
    v_fname varchar2(2000);
    -- 秒数
    v_seconds varchar2(100);
    -- 数据采集系统类型
    v_system varchar2(10);
    -- 数据产生源标识
    v_source varchar2(40);
    -- 序列号
    v_seq_num number(10);
    -- 数据集代码
    v_data_code varchar2(200);
    -- 结构化标识
    v_structed number(1);
    v_path      varchar2(40);
    -- 文件后缀
    v_suffix varchar2(10);
    -- 数据库抽取记录表记录的类型
    v_extract_type varchar2(256);
    -- 数据库抽取记录表记录的code
    v_extract_code varchar2(256);
    -- 表名
    v_tname varchar2(256);

    -------------------------sql相关变量---------------------------------
    -- 要查询的sql模板，后面会进行动态拼接
    v_sql_select_temp  varchar2(2000);
    -- 要查询的sql
    v_sql_select varchar2(2000);
    --查询条件
    v_where varchar2(2000);
    -- 统计起始截止ID之间有多少数据模板
    v_sql_cnt_temp varchar2(2000);
    -- 统计起始截止ID之间有多少数据
    v_sql_cnt varchar2(2000);
    -- 统计查询出来的条数
    v_select_cnt number(10);

    -------------------------id相关变量---------------------------------
    -- 抽取起始id
    v_id_start number(10);
    -- 抽取结束id
    v_id_end number(10);
    -- id偏移量
    v_id_offset number(10);
    -- 一次抽取多少数据
    v_row_nums number(10);
    -- 统计写入文件多少条数据了
    v_file_cnt number(10);

    -------------------------临时变量---------------------------------
    -- 临时字符串
    v_str_temp varchar2(2000);
    -- 临时数字
    v_num_temp number(10);

    -------------------------其他变量---------------------------------
    type type_cursor is ref cursor;
    mycursor type_cursor;
    -- 查询出的一条数据
    -- v_record mycursor%ROWTYPE;
    outf     utl_file.file_type;
begin
    -------------------------------------------------初始化变量---------------------------------------------------------
    -- 取消Oracle内存缓冲区大小限制
    DBMS_OUTPUT.ENABLE(buffer_size => null);

    -- 数据采集系统类型
    v_system   := '120';
    -- 数据产生源标识
    v_source := '510500';

    -- 5位自增序列号
    v_seq_num := 0;
    -- 结构化/非结构化标识(0表示结构化标识-默认, 1表示非结构化标识)
    v_structed := '0';
    -- 数据导出文件的后缀
    v_suffix   := '.bcp';
    -- 数据库记录任务类型
    select upper('extract') into v_extract_type from dual;
    -- 表名大写
    v_str_temp := 'select upper(''' || v_table_name || ''') from dual';
    execute immediate v_str_temp into v_tname;

    -- 数据库记录任务类型
    select upper('luzhou_extract_') || v_tname into v_extract_code from dual;

    -- 一个导出文件存储多少条数据,默认1000条
    v_row_nums     := 1000;
    --
    v_num_temp := 0;
    v_file_cnt := 0;

    -- 指定下载文件的路径
    select directory_path into v_path from dba_directories where directory_name = 'DIR_EXTRA_PATH';

    -- 起始ID,先从数据库抽取记录表里查询是否有抽取记录，
    -- 如果有抽取记录从记录的位置开始抽取，如果没有抽取记录，则从开头始抽取
    v_str_temp := 'select to_number(nvl((select param_value from db_parameter where param_type = ''' || v_extract_type || ''' and param_code = ''' || v_extract_code || '''),''-1'')) from dual';
    execute immediate v_str_temp into v_id_start;

    v_str_temp := 'select nvl(min(id), 0) from ' || v_tname ;
    execute immediate v_str_temp into v_num_temp;

    if v_id_start < 0 then
        dbms_output.put_line('db_parameter表里没有抽取记录,将从头始抽取');
        v_id_start := v_num_temp;
        -- 抽取记录写入数据库
        v_str_temp := 'insert into db_parameter(param_type, param_code, param_name, param_value) values (''' || v_extract_type || ''', ''' || v_extract_code || ''', ''extract max id record of table ' || v_tname || ''', ''' || v_id_start || ''')';
        execute immediate v_str_temp;
        v_seq_num := 0;
        commit;
    else
        -- 如果数据库中有抽取记录,则生成的BCP文件的序列号继续递增
        v_str_temp := 'select ceil(''' || ((v_id_start - v_num_temp) / v_row_nums) || ''') from dual';
        execute immediate v_str_temp into v_seq_num;
    end if;

    -- 结束ID，如果用户指定的ID大于表中最大的ID，则替换为表中最大的ID,如果表中没有数据则直接退出
    v_str_temp := 'select nvl(max(id), 0) from ' || v_tname ;
    execute immediate v_str_temp into v_num_temp;
    if v_num_temp = 0 then
       dbms_output.put_line('表' || v_tname || '中没有数据,程序即将退出');
       return;
    elsif v_num_temp < v_iend then
       dbms_output.put_line('您设定的最大ID -->' || v_id_end || '大于表中最大的ID --> ' || v_num_temp || '程序将抽取表中所有数据');
       v_id_end := v_num_temp;
    else
       v_id_end := v_iend;
    end if;

    dbms_output.put_line('开始ID --> ' || v_id_start);
    -- dbms_output.put_line('偏移ID --> ' || v_id_offset);
    dbms_output.put_line('结束ID --> ' || v_id_end);

    -- 查询条件
    v_where := ' where id >= id_start and id < id_end';
    v_sql_cnt_temp := 'select count(1) from '|| v_tname || v_where;


    -- 根据表名获取字段名及字段类型
    if v_tname = 'ENDING_LOGINOUT' then
        v_sql_select_temp := 'SERVICE_CODE,CERTIFICATE_TYPE,CERTIFICATE_CODE,USER_NAME,ROOM_ID,to_char(LOGIN_TIME@ ''yyyy-mm-dd-hh24:mi:ss''),to_char(LOGOUT_TIME@ ''yyyy-mm-dd-hh24:mi:ss''),decode(LOGOUT_TIME@ null@ 1@ 0)||chr(9)';
        -- 数据集代码
        v_data_code := 'WA_SOURCE_FJ_0001';
    elsif v_tname = 'VID_LOGINOUT' then
        v_sql_select_temp := 'SERVICE_CODE,CERTIFICATE_TYPE,CERTIFICATE_CODE,USER_NAME,ROOM_ID,PROTOCOL_TYPE,ACCOUNT,to_char(LOGIN_TIME@ ''yyyy-mm-dd-hh24:mi:ss'')||chr(9)';
        -- 数据集代码
        v_data_code := 'WA_SOURCE_0012';
    end if;
    -- 先替换制表符，再替换逗号,直接写的话太长还不容易理解
    v_sql_select_temp := replace(v_sql_select_temp, ',', '||chr(9)||');
    v_sql_select_temp := replace(v_sql_select_temp, '@', ',');

    -- 去数据库查询sql的模板,返回两个字段,一个是主键id,另外一个是拼装好的要写入到数据文件的内容,格式为:
    -- select id, column_values as info from tablename where id >= id_start
    -- 截止id后面会再进行拼装
    v_sql_select_temp := 'select id, ' || v_sql_select_temp || ' as info from ' || v_tname || v_where;
    -------------------------------------------------初始化变量完成-----------------------------------------------------
    -- 打印要执行的sql
    -- dbms_output.put_line(v_sql_select_temp);

    -------------------------------------------------Oracle抽取数据到BCP文件---------------------------------------------
    -- 生成文件名
    -- v_seconds绝对秒数时间
    select ceil((sysdate - to_date('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:mi:ss')) * 24 * 3600) into v_seconds from dual;
    -- 生成的BCP文件数加1
    v_seq_num := v_seq_num + 1;
    -- 生成BCP文件名
    v_fname := v_system || chr(45) || v_source || chr(45) || v_seconds ||
             chr(45) || replace(lpad(v_seq_num, 5), ' ', '0') || chr(45) || v_data_code || chr(45) ||
             v_structed || v_suffix;
    -- dbms_output.put_line('生成的BCP文件名 --> ' || v_fname);
    if v_id_start > v_id_end then       --如果起始ID大于截止ID直接退出
        dbms_output.put_line('截止ID '|| v_id_end || ' 小于起始ID ' || v_id_start || '程序即将退出');
        return;
    else
        -- 打开要写入的文件
        outf := utl_file.fopen(v_path, v_fname, 'a');
    end if;

    while true
    loop
        -- 根据sql查询模板生成要执行的sql
        -- 替换起始ID
        v_sql_select := replace(v_sql_select_temp, 'id_start', v_id_start);
        v_sql_cnt := replace(v_sql_cnt_temp, 'id_start', v_id_start);

        if (v_id_end - v_id_start) > v_row_nums then
            -- 如果截止ID减起始ID大于默认最大行数,偏移量取最大行数据
            v_id_offset := v_row_nums;
        else
            -- 如果截止ID减起始ID小于默认最大行数,偏移量取差值,这样的考虑是有时候ID自增的序列步长大于1
            v_id_offset := (v_id_end - v_id_start + 1);
        end if;
        v_sql_select := replace(v_sql_select, 'id_end', (v_id_start + v_id_offset));
        v_sql_cnt := replace(v_sql_cnt, 'id_end', (v_id_start + v_id_offset));
        -- dbms_output.put_line('查询SQL --> ' || v_sql_select);

        -- 如果起始ID和截止ID之间没有数据则跳过，不创建文件
        execute immediate v_sql_cnt into v_select_cnt;
        if v_select_cnt = 0 then
          v_id_start := v_id_start + v_id_offset;
          -- 跳出循环
          if v_id_start >= v_id_end then
              dbms_output.put_line('Oracle数据抽取BCP文件完成');
              exit;
          else
            continue;
          end if;
        end if;

        -- 打开游标读取查询数据
        open mycursor for v_sql_select;
        loop
            fetch mycursor
                into v_num_temp, v_str_temp;
                exit when mycursor%notfound;

            -- dbms_output.put_line(v_num_temp || ' --> ' || v_str_temp);
            if utl_file.is_open(outf) then
              utl_file.put_line(outf, v_str_temp);
              v_file_cnt := v_file_cnt + 1;
            end if;

            -- 打开要写入的文件
            if v_file_cnt >= v_row_nums then
                -- 关闭文件IO
                if utl_file.is_open(outf) then
                   utl_file.fclose(outf);
                end if;

                -- 生成BCP文件名
                -- v_seconds绝对秒数时间
                select ceil((sysdate - to_date('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:mi:ss')) * 24 * 3600) into v_seconds from dual;
                -- 生成的BCP文件数加1
                v_seq_num := v_seq_num + 1;
                -- 生成BCP文件名
                v_fname := v_system || chr(45) || v_source || chr(45) || v_seconds ||
                         chr(45) || replace(lpad(v_seq_num, 5), ' ', '0') || chr(45) || v_data_code || chr(45) ||
                         v_structed || v_suffix;
                -- dbms_output.put_line('生成的BCP文件名 --> ' || v_fname);
                outf := utl_file.fopen(v_path, v_fname, 'a');
                v_file_cnt := 0;
            end if;
        end loop;
        v_id_start := v_id_start + v_id_offset;
        if v_seq_num >= 99999 then
            v_seq_num := 1;
        end if;



        -- 更新抽取记录
        v_str_temp := 'update db_parameter set param_value = ' || v_id_start || ' where param_type = upper(''extract'') and param_code = upper(''luzhou_extract_' || v_tname || ''')';
        execute immediate v_str_temp;
        commit;

        dbms_output.put_line(v_fname || ' 导出成功');

        -- 跳出循环
        if v_id_start >= v_id_end then
            dbms_output.put_line('Oracle数据抽取BCP文件完成');
            -- 关闭文件IO
            if utl_file.is_open(outf) then
               utl_file.fclose(outf);
            end if;
            exit;
        end if;
    end loop;
    -------------------------------------------------Oracle抽取数据完成---------------------------------------------
    -- 异常处理
    exception
       when others then
           rollback;
end;
end;
