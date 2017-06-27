package com.rainsoft.domain;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RegContentSearch {
    public String id;
    public String sessionid;
    public String service_code;
    public String room_id;
    public String certificate_type;
    public String certificate_code;
    public String user_name;
    public String protocol_type;
    public String url;
    public String domain_name;
    public String ref_url;
    public String ref_domain;
    public String keyword;
    public String keyword_code;
    public String dest_ip;
    public String dest_port;
    public String src_ip;
    public String src_port;
    public String src_mac;
    public String capture_time;
    public String checkin_id;
    public String machine_id;

    @Override
    public String toString() {
        return "RegContentSearch{" +
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
                ", keyword='" + keyword + '\'' +
                ", keyword_code='" + keyword_code + '\'' +
                ", dest_ip='" + dest_ip + '\'' +
                ", dest_port='" + dest_port + '\'' +
                ", src_ip='" + src_ip + '\'' +
                ", src_port='" + src_port + '\'' +
                ", src_mac='" + src_mac + '\'' +
                ", capture_time='" + capture_time + '\'' +
                ", checkin_id='" + checkin_id + '\'' +
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

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getKeyword_code() {
        return keyword_code;
    }

    public void setKeyword_code(String keyword_code) {
        this.keyword_code = keyword_code;
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

    public String getMachine_id() {
        return machine_id;
    }

    public void setMachine_id(String machine_id) {
        this.machine_id = machine_id;
    }
}
