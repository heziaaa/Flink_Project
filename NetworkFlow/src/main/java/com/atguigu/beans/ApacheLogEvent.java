package com.atguigu.beans;

public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long time;
    private String method;
    private String url;

    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String ip, String userId, Long time, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.time = time;
        this.method = method;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", time=" + time +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
