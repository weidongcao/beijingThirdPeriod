create or replace procedure proc_advance_warning () as
BEGIN
insert into offline_warning(SERVICE_CODE, OFFLINE_TIME, WARNING_LEVEL, CHECK_MODE)
select
    service_code,
    update_time,  -- 更新时间
    case
    when  ceil((sysdate - update_time) * 24) < 72 THEN 1   -- 不足72小时为级别1
    WHEN ceil((sysdate - update_time) * 24) > 72 THEN 2    -- 超过72小时为级别2
    end case,
    0
from
    SERVICE_INFO
where
    -- 场所离线
    online_status = '0'
    -- 超过1小时
    and ceil((sysdate - update_time) * 24) > 1
    -- 场所不在预警表里没有处理的场所集合里
    and service_code not in (select service_code from offline_warning where is_checked = 0);

-- 预警表里is_checked为0(没有处理),超过72小时,但是预警级别为1的,
-- 预警级别(warning_level)更新为2,处理级别(check_mode)更新为2
update OFFLINE_WARNING
SET WARNING_LEVEL = 2, CHECK_MODE = 2
WHERE
    is_checked = 0
    and WARNING_LEVEL = 1
    and ceil((sysdate - offline_time) * 24) > 72;
commit;
end;