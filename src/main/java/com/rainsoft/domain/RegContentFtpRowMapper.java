package com.rainsoft.domain;

import org.springframework.jdbc.core.RowMapper;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Administrator on 2017-06-12.
 */
public class RegContentFtpRowMapper implements RowMapper<RegContentFtp>, Serializable {


    private static final long serialVersionUID = 1561235458163021279L;

    @Override
    public RegContentFtp mapRow(ResultSet rs, int i) throws SQLException {
        RegContentFtp ftp = new RegContentFtp();
        ftp.setId(rs.getString(0));
        ftp.setSessionid(rs.getString(1));
        ftp.setService_code(rs.getString(2));
        ftp.setRoom_id(rs.getString(3));
        ftp.setCertificate_type(rs.getString(4));
        ftp.setCertificate_code(rs.getString(5));
        ftp.setUser_name(rs.getString(6));
        ftp.setProtocol_type(rs.getString(7));
        ftp.setAccount(rs.getString(8));
        ftp.setPasswd(rs.getString(9));
        ftp.setFile_name(rs.getString(10));
        ftp.setFile_path(rs.getString(11));
        ftp.setAction_type(rs.getString(12));
        ftp.setIs_completed(rs.getString(13));
        ftp.setDest_ip(rs.getString(14));
        ftp.setDest_port(rs.getString(15));
        ftp.setSrc_ip(rs.getString(16));
        ftp.setSrc_port(rs.getString(17));
        ftp.setSrc_mac(rs.getString(18));
        ftp.setCapture_time(rs.getString(19));
        ftp.setCheckin_id(rs.getString(20));
        ftp.setData_source(rs.getString(21));
        ftp.setMachine_id(rs.getString(22));

        return ftp;
    }
}
