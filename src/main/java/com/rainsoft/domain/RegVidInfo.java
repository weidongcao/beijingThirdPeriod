package com.rainsoft.domain;

/**
 * Created by CaoWeiDong on 2017-06-28.
 */
public class RegVidInfo {
    private String protocol_type;
    private String account;
    private String nick_name;
    private String passwd;
    private String use_nums;
    private String last_logintime;
    private String last_service_code;
    private String first_logintime;
    private String first_service_code;
    private String first_room_id;
    private String last_room_id;
    private String realid_nums;
    private String id;

    @Override
    public String toString() {
        return "RegVidInfo{" +
                "protocol_type='" + protocol_type + '\'' +
                ", account='" + account + '\'' +
                ", nick_name='" + nick_name + '\'' +
                ", passwd='" + passwd + '\'' +
                ", use_nums='" + use_nums + '\'' +
                ", last_logintime='" + last_logintime + '\'' +
                ", last_service_code='" + last_service_code + '\'' +
                ", first_logintime='" + first_logintime + '\'' +
                ", first_service_code='" + first_service_code + '\'' +
                ", first_room_id='" + first_room_id + '\'' +
                ", last_room_id='" + last_room_id + '\'' +
                ", realid_nums='" + realid_nums + '\'' +
                ", id='" + id + '\'' +
                '}';
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

    public String getNick_name() {
        return nick_name;
    }

    public void setNick_name(String nick_name) {
        this.nick_name = nick_name;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getUse_nums() {
        return use_nums;
    }

    public void setUse_nums(String use_nums) {
        this.use_nums = use_nums;
    }

    public String getLast_logintime() {
        return last_logintime;
    }

    public void setLast_logintime(String last_logintime) {
        this.last_logintime = last_logintime;
    }

    public String getLast_service_code() {
        return last_service_code;
    }

    public void setLast_service_code(String last_service_code) {
        this.last_service_code = last_service_code;
    }

    public String getFirst_logintime() {
        return first_logintime;
    }

    public void setFirst_logintime(String first_logintime) {
        this.first_logintime = first_logintime;
    }

    public String getFirst_service_code() {
        return first_service_code;
    }

    public void setFirst_service_code(String first_service_code) {
        this.first_service_code = first_service_code;
    }

    public String getFirst_room_id() {
        return first_room_id;
    }

    public void setFirst_room_id(String first_room_id) {
        this.first_room_id = first_room_id;
    }

    public String getLast_room_id() {
        return last_room_id;
    }

    public void setLast_room_id(String last_room_id) {
        this.last_room_id = last_room_id;
    }

    public String getRealid_nums() {
        return realid_nums;
    }

    public void setRealid_nums(String realid_nums) {
        this.realid_nums = realid_nums;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
