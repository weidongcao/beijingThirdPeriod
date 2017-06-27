package com.rainsoft.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Administrator on 2017-06-12.
 */
public class RegContentFtp implements Serializable {

    private static final long serialVersionUID = 2236145089427356882L;

    //ID
    public String id;
    //会话ID
    public String sessionid;
    //场所编号
    public String service_code;
    //房间号/座位号
    public String room_id;
    //证件类型
    public String certificate_type;
    //证件号
    public String certificate_code;
    //证件姓名
    public String user_name;
    //协议类型
    public String protocol_type;
    //帐号
    public String account;
    //密码
    public String passwd;
    //文件名
    public String file_name;
    //文件路径
    public String file_path;
    //操作类型
    public String action_type;
    //是否完成
    public String is_completed;
    //目标IP
    public String dest_ip;
    //目标端口
    public String dest_port;
    //源IP
    public String src_ip;
    //源端口
    public String src_port;
    //源MAC地址
    public String src_mac;
    //捕获时间
    public String capture_time;
    //用户登记ID
    public String checkin_id;
    //数据来源
    public String data_source;
    //设备编号
    public String machine_id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public String getService_code() {
        return service_code;
    }

    public void setService_code(String service_code) {
        this.service_code = service_code;
    }

    public String getRoom_id() {
        return room_id;
    }

    public void setRoom_id(String room_id) {
        this.room_id = room_id;
    }

    public String getCertificate_type() {
        return certificate_type;
    }

    public void setCertificate_type(String certificate_type) {
        this.certificate_type = certificate_type;
    }

    public String getCertificate_code() {
        return certificate_code;
    }

    public void setCertificate_code(String certificate_code) {
        this.certificate_code = certificate_code;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getProtocol_type() {
        return protocol_type;
    }

    public void setProtocol_type(String protocol_type) {
        this.protocol_type = protocol_type;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getAction_type() {
        return action_type;
    }

    public void setAction_type(String action_type) {
        this.action_type = action_type;
    }

    public String getIs_completed() {
        return is_completed;
    }

    public void setIs_completed(String is_completed) {
        this.is_completed = is_completed;
    }

    public String getDest_ip() {
        return dest_ip;
    }

    public void setDest_ip(String dest_ip) {
        this.dest_ip = dest_ip;
    }

    public String getDest_port() {
        return dest_port;
    }

    public void setDest_port(String dest_port) {
        this.dest_port = dest_port;
    }

    public String getSrc_ip() {
        return src_ip;
    }

    public void setSrc_ip(String src_ip) {
        this.src_ip = src_ip;
    }

    public String getSrc_port() {
        return src_port;
    }

    public void setSrc_port(String src_port) {
        this.src_port = src_port;
    }

    public String getSrc_mac() {
        return src_mac;
    }

    public void setSrc_mac(String src_mac) {
        this.src_mac = src_mac;
    }

    public String getCapture_time() {
        return capture_time;
    }

    public void setCapture_time(String capture_time) {
        this.capture_time = capture_time;
    }

    public String getCheckin_id() {
        return checkin_id;
    }

    public void setCheckin_id(String checkin_id) {
        this.checkin_id = checkin_id;
    }

    public String getData_source() {
        return data_source;
    }

    public void setData_source(String data_source) {
        this.data_source = data_source;
    }

    public String getMachine_id() {
        return machine_id;
    }

    public void setMachine_id(String machine_id) {
        this.machine_id = machine_id;
    }

    @Override
    public String toString() {
        return "RegContentFtp{" +
                "id='" + id + '\'' +
                ", sessionid='" + sessionid + '\'' +
                ", service_code='" + service_code + '\'' +
                ", room_id='" + room_id + '\'' +
                ", certificate_type='" + certificate_type + '\'' +
                ", certificate_code='" + certificate_code + '\'' +
                ", user_name='" + user_name + '\'' +
                ", protocol_type='" + protocol_type + '\'' +
                ", account='" + account + '\'' +
                ", passwd='" + passwd + '\'' +
                ", file_name='" + file_name + '\'' +
                ", file_path='" + file_path + '\'' +
                ", action_type='" + action_type + '\'' +
                ", is_completed='" + is_completed + '\'' +
                ", dest_ip='" + dest_ip + '\'' +
                ", dest_port='" + dest_port + '\'' +
                ", src_ip='" + src_ip + '\'' +
                ", src_port='" + src_port + '\'' +
                ", src_mac='" + src_mac + '\'' +
                ", capture_time='" + capture_time + '\'' +
                ", checkin_id='" + checkin_id + '\'' +
                ", data_source='" + data_source + '\'' +
                ", machine_id='" + machine_id + '\'' +
                '}';
    }
}
