package com.rainsoft.domain;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RegContentEmail {
    private String id;
    private String sessionid;
    private String service_code;
    private String room_id;
    private String certificate_type;
    private String certificate_code;
    private String user_name;
    private String protocol_type;
    private String account;
    private String passwd;
    private String mailid;
    private String send_time;
    private String mail_from;
    private String mail_to;
    private String cc;
    private String bcc;
    private String subject;
    private String summary;
    private String attachment;
    private String file_path;
    private String action_type;
    private String dest_ip;
    private String dest_port;
    private String src_ip;
    private String src_port;
    private String src_mac;
    private String capture_time;
    private String checkin_id;
    private String data_source;
    private String machine_id;

    @Override
    public String toString() {
        return "RegContentEmail{" +
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
                ", mailid='" + mailid + '\'' +
                ", send_time='" + send_time + '\'' +
                ", mail_from='" + mail_from + '\'' +
                ", mail_to='" + mail_to + '\'' +
                ", cc='" + cc + '\'' +
                ", bcc='" + bcc + '\'' +
                ", subject='" + subject + '\'' +
                ", summary='" + summary + '\'' +
                ", attachment='" + attachment + '\'' +
                ", file_path='" + file_path + '\'' +
                ", action_type='" + action_type + '\'' +
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

    public String getMailid() {
        return mailid;
    }

    public void setMailid(String mailid) {
        this.mailid = mailid;
    }

    public String getSend_time() {
        return send_time;
    }

    public void setSend_time(String send_time) {
        this.send_time = send_time;
    }

    public String getMail_from() {
        return mail_from;
    }

    public void setMail_from(String mail_from) {
        this.mail_from = mail_from;
    }

    public String getMail_to() {
        return mail_to;
    }

    public void setMail_to(String mail_to) {
        this.mail_to = mail_to;
    }

    public String getCc() {
        return cc;
    }

    public void setCc(String cc) {
        this.cc = cc;
    }

    public String getBcc() {
        return bcc;
    }

    public void setBcc(String bcc) {
        this.bcc = bcc;
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

    public String getAttachment() {
        return attachment;
    }

    public void setAttachment(String attachment) {
        this.attachment = attachment;
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
}
