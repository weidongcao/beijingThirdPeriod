package com.rainsoft.domain;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Administrator on 2017-06-15.
 */
public class RegContentHttpRowMapper implements RowMapper {
    @Override
    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
        RegContentHttp http = new RegContentHttp();
        http.setId(rs.getString(1));
        http.setSessionid(rs.getString(2));
        http.setService_code(rs.getString(3));
        http.setRoom_id(rs.getString(4));
        http.setCertificate_type(rs.getString(5));
        http.setCertificate_code(rs.getString(6));
        http.setUser_name(rs.getString(7));
        http.setProtocol_type(rs.getString(8));
        http.setUrl(rs.getString(9));
        http.setDomain_name(rs.getString(10));
        http.setRef_url(rs.getString(11));
        http.setRef_domain(rs.getString(12));
        http.setAction_type(rs.getString(13));
        http.setSubject(rs.getString(14));
        http.setSummary(rs.getString(15));
        http.setCookie_path(rs.getString(16));
        http.setUpload_file(rs.getString(17));
        http.setDownload_file(rs.getString(18));
        http.setDest_ip(rs.getString(19));
        http.setDest_port(rs.getString(20));
        http.setSrc_ip(rs.getString(21));
        http.setSrc_port(rs.getString(22));
        http.setSrc_mac(rs.getString(23));
        http.setCapture_time(rs.getString(24));
        http.setCheckin_id(rs.getString(25));
        http.setData_source(rs.getString(26));
        http.setMachine_id(rs.getString(27));
        http.setImport_time(rs.getString("import_time"));
        return http;
    }
}
