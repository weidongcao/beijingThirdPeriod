-- 还存在的问题:
-- 上下线信息realid_loginout 最后一个字段文档给的是STATUS(在线状态)但是在表中没有找到，暂时以ICCARD_TYPE替代

--创建路径 
--create or replace directory DIR_EXTRA_PATH as '/opt/data/lyzhou';

-- 创建存储过程
create or replace procedure proc_extra_bcp (
       v_tname in varchar, --表名
       v_id_end in number  -- 结束ID
) 
as
begin
declare
    -------------------------BCP文件相关变量---------------------------------
    -- 生成的数据文件名
    v_fname varchar2(2000);
    --秒数
    v_seconds varchar2(100);
    --数据采集系统类型
    v_system varchar2(10);
    --数据产生源标识
    v_source varchar2(40);
    --序列号
    v_seq_num number(10);
    --数据集代码
    v_data_code varchar2(200);
    --结构化标识
    v_structed number(1);
    v_path      varchar2(40);
    --文件后缀
    v_suffix varchar2(10);

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
    -- id偏移量
    v_id_offset number(10);
    --一次抽取多少数据
    v_row_nums number(10);


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
    --取消Oracle内存缓冲区大小限制
    DBMS_OUTPUT.ENABLE(buffer_size => null);

    -- 起始ID
    v_str_temp := 'select min(id) - 1 from ' || v_tname;
    execute immediate v_str_temp into v_id_start;

    -- 数据采集系统类型
    v_system   := '120';
    -- 数据产生源标识
    v_source := '510500';
    --v_seconds绝对秒数时间
    select ceil(sysdate - to_date('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:mi:ss')) * 24 * 3600 into v_seconds from dual;
    -- 5位自增序列号
    v_seq_num := 0;
    -- 结构化/非结构化标识(0表示结构化标识-默认, 1表示非结构化标识)
    v_structed := '0';
    -- 数据导出文件的后缀
    v_suffix   := '.bcp';

    -- 一个导出文件存储多少条数据,默认1000条
    v_row_nums     := 1000;
    --
    v_num_temp := 0;

    -- 指定下载文件的路径
    select directory_path into v_path from dba_directories where directory_name = 'DIR_EXTRA_PATH';

    dbms_output.put_line('开始ID --> ' || v_id_start);
    --dbms_output.put_line('偏移ID --> ' || v_id_offset);
    dbms_output.put_line('结束ID --> ' || v_id_end);

    --查询条件
    v_where := ' where id >= id_start and id < id_end';
    v_sql_cnt_temp := 'select count(1) from '||v_tname || v_where;


    --根据表名获取字段名及字段类型
    if v_tname = 'realid_loginout' then
        --v_sql_select_temp := 'SERVICE_CODE||chr(9)||CERTIFICATE_TYPE||chr(9)||CERTIFICATE_CODE||chr(9)||USER_NAME||chr(9)||ROOM_ID||chr(9)||to_char(LOGIN_TIME, ''yyyy-mm-dd-hh24:mi:ss'')||chr(9)||to_char(LOGOUT_TIME, ''yyyy-mm-dd-hh24:mi:ss'')||chr(9)||ICCARD_TYPE||chr(9)';
        v_sql_select_temp := 'SERVICE_CODE,CERTIFICATE_TYPE,CERTIFICATE_CODE,USER_NAME,ROOM_ID,to_char(LOGIN_TIME@ ''yyyy-mm-dd-hh24:mi:ss''),to_char(LOGOUT_TIME@ ''yyyy-mm-dd-hh24:mi:ss''),ICCARD_TYPE||chr(9)';
        -- 数据集代码
        v_data_code := 'WA_SOURCE_FJ_0001';
    elsif v_tname = 'vid_loginout' then
        v_sql_select_temp := 'SERVICE_CODE,CERTIFICATE_TYPE,CERTIFICATE_CODE,USER_NAME,ROOM_ID,PROTOCOL_TYPE,ACCOUNT,to_char(LOGOUT_TIME@ ''yyyy-mm-dd-hh24:mi:ss'')||chr(9)';
        -- 数据集代码
        v_data_code := 'WA_SOURCE_0012';
    end if;
    --先替换制表符，再替换逗号,直接写的话太长还不容易理解
    v_sql_select_temp := replace(v_sql_select_temp, ',', '||chr(9)||');
    v_sql_select_temp := replace(v_sql_select_temp, '@', ',');

    --去数据库查询sql的模板,返回两个字段,一个是主键id,另外一个是拼装好的要写入到数据文件的内容,格式为:
    --select id, column_values as info from tablename where id >= id_start
    -- 截止id后面会再进行拼装
    v_sql_select_temp := 'select id, ' || v_sql_select_temp || ' as info from ' || v_tname || v_where;
    -------------------------------------------------初始化变量完成-----------------------------------------------------
    --打印要执行的sql
    --dbms_output.put_line(v_sql_select_temp);

    -------------------------------------------------Oracle抽取数据到BCP文件---------------------------------------------
    while true
    loop
        --根据sql查询模板生成要执行的sql
        --替换起始ID
        v_sql_select := replace(v_sql_select_temp, 'id_start', v_id_start);
        v_sql_cnt := replace(v_sql_cnt_temp, 'id_start', v_id_start);
        if v_id_start > v_id_end then       --如果起始ID大于截止ID直接退出
            dbms_output.put_line('截止ID '|| v_id_end || ' 小于起始ID ' || v_id_start || '程序即将退出');
            exit;
        elsif (v_id_end - v_id_start) > v_row_nums then     --如果截止ID减起始ID大于默认最大行数,偏移量取最大行数据
            v_id_offset := v_row_nums;
        else
            v_id_offset := (v_id_end - v_id_start + 1);         --如果截止ID减起始ID小于默认最大行数,偏移量取差值,这样的考虑是有时候ID自增的序列步长大于1
        end if;
        v_sql_select := replace(v_sql_select, 'id_end', (v_id_start + v_id_offset));
        v_sql_cnt := replace(v_sql_cnt, 'id_end', (v_id_start + v_id_offset));
        --dbms_output.put_line('查询SQL --> ' || v_sql_select);

        --如果起始ID和截止ID之间没有数据则跳过，不创建文件
        execute immediate v_sql_cnt into v_select_cnt;
        if v_select_cnt = 0 then
          v_id_start := v_id_start + v_row_nums;
          --跳出循环
          if v_id_start >= v_id_end then
              dbms_output.put_line('Oracle数据抽取BCP文件完成');
              exit;
          else
            continue;
          end if;
        end if;
        
        --生成的BCP文件数加1
        v_seq_num := v_seq_num + 1;
        --生成BCP文件名
        v_fname := v_system || chr(45) || v_source || chr(45) || v_seconds ||
                 chr(45) || replace(lpad(v_seq_num, 5), ' ', '0') || chr(45) || v_data_code || chr(45) ||
                 v_structed || v_suffix;
        --dbms_output.put_line('生成的BCP文件名 --> ' || v_fname);
        
        -- 打开要写入的文件
        outf := utl_file.fopen(v_path, v_fname, 'a');
        --打开游标读取查询数据
        open mycursor for v_sql_select;
        loop
            fetch mycursor
                into v_num_temp, v_str_temp;
                exit when mycursor%notfound;

            --dbms_output.put_line(v_num_temp || ' --> ' || v_str_temp);
            if utl_file.is_open(outf) then
              utl_file.put_line(outf, v_str_temp);
            end if;
        end loop;
        v_id_start := v_id_start + v_row_nums;

        -- 关闭文件IO
        if utl_file.is_open(outf) then
           utl_file.fclose(outf);
        end if;

        dbms_output.put_line(v_fname || ' 导出成功');

        --跳出循环
        if v_id_start >= v_id_end then
            dbms_output.put_line('Oracle数据抽取BCP文件完成');
            exit;
        end if;
    end loop;
    -------------------------------------------------Oracle抽取数据完成---------------------------------------------
    --异常处理
    exception
       when others then
       null;
end;
end;


