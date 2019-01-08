procedure pro_daily_busi_service_stats
as
begin
    delete from  daily_business_statics where stats_date<trunc(sysdate)-7;
    insert into daily_business_statics(service_code, login_times, stats_date, create_time)
    SELECT
        nvl(a.SERVICE_CODE, b.SERVICE_CODE),
        nvl(a.login_times, 0),
        nvl(b.ending_nums, 0),
        trunc(sysdate) - 1,
        sysdate
    from
    (
        select
            SERVICE_CODE,
            count(DISTINCT  CHECKIN_ID) as login_times
        from
            reg_ending_loginout
        WHERE
            LOGIN_TIME >= trunc(sysdate) - 1
            and LOGIN_TIME < trunc(sysdate)
        GROUP BY
            SERVICE_CODE
    ) a
    FULL JOIN
    (
        SELECT
            SERVICE_CODE,
            count(ending_mac) as ending_nums
        from
            service_ending_status
        where
            create_time >= trunc(sysdate) - 15/24
            and create_time < trunc(sysdate)
        GROUP BY
            SERVICE_CODE
    ) b
    ON
        a.SERVICE_CODE = b.service_code
    commit;
    exception
        when others then
            rollback;
            raise;
end pro_daily_busi_service_stats;