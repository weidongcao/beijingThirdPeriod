package com.rainsoft.domain;

import java.io.Serializable;

/**
 * Created by Administrator on 2017-06-12.
 */
public class RegContentHttp implements Serializable{


    private static final long serialVersionUID = -5949001294450481485L;
    //ID
    public String id = "";
    //会话ID
    public String sessionid = "";
    //场所编号
    public String service_code = "";
    //房间号
    public String room_id = "";
    //证件类型
    public String certificate_type = "";
    //证件号
    public String certificate_code = "";
    //证件姓名
    public String user_name = "";
    //协议类型
    public String protocol_type = "";
    //
    public String url = "";
    //域名
    public String domain_name = "";
    //引用URL
    public String ref_url = "";
    //引用域名
    public String ref_domain = "";
    //操作类型1=上线；2=下线；3=订单；4=注册信息
    public String action_type = "";
    //页面主题
    public String subject = "";
    //正文摘要
    public String summary = "";
    //cookie路径
    public String cookie_path = "";
    //上传文件
    public String upload_file = "";
    //下载文件
    public String download_file = "";
    //目标IP
    public String dest_ip = "";
    //目标端口
    public String dest_port = "";
    //源IP
    public String src_ip = "";
    //源端口
    public String src_port = "";
    //源MAC地址
    public String src_mac = "";
    //捕获时间
    public String capture_time = "";
    //用户登记ID
    public String checkin_id = "";
    //数据来源
    public String data_source = "";
    //设备编号
    public String machine_id = "";

    public String file_path = "";
    public String passwd = "";
    public String posting_id = "";

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getPosting_id() {
        return posting_id;
    }

    public void setPosting_id(String posting_id) {
        this.posting_id = posting_id;
    }

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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDomain_name() {
        return domain_name;
    }

    public void setDomain_name(String domain_name) {
        this.domain_name = domain_name;
    }

    public String getRef_url() {
        return ref_url;
    }

    public void setRef_url(String ref_url) {
        this.ref_url = ref_url;
    }

    public String getRef_domain() {
        return ref_domain;
    }

    public void setRef_domain(String ref_domain) {
        this.ref_domain = ref_domain;
    }

    public String getAction_type() {
        return action_type;
    }

    public void setAction_type(String action_type) {
        this.action_type = action_type;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getCookie_path() {
        return cookie_path;
    }

    public void setCookie_path(String cookie_path) {
        this.cookie_path = cookie_path;
    }

    public String getUpload_file() {
        return upload_file;
    }

    public void setUpload_file(String upload_file) {
        this.upload_file = upload_file;
    }

    public String getDownload_file() {
        return download_file;
    }

    public void setDownload_file(String download_file) {
        this.download_file = download_file;
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
        return "RegContentHttp{" +
                "id='" + id + '\'' +
                ", sessionid='" + sessionid + '\'' +
                ", service_code='" + service_code + '\'' +
                ", room_id='" + room_id + '\'' +
                ", certificate_type='" + certificate_type + '\'' +
                ", certificate_code='" + certificate_code + '\'' +
                ", user_name='" + user_name + '\'' +
                ", protocol_type='" + protocol_type + '\'' +
                ", url='" + url + '\'' +
                ", domain_name='" + domain_name + '\'' +
                ", ref_url='" + ref_url + '\'' +
                ", ref_domain='" + ref_domain + '\'' +
                ", action_type='" + action_type + '\'' +
                ", subject='" + subject + '\'' +
                ", summary='" + summary + '\'' +
                ", cookie_path='" + cookie_path + '\'' +
                ", upload_file='" + upload_file + '\'' +
                ", download_file='" + download_file + '\'' +
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
