package com.example.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "seat-routing")
public class SeatRoutingProperties {
    private String cookie;
    private String localNode;
    private String nodeRes1;
    private String nodeRes2;
    private String nodeRes3;

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public String getLocalNode() {
        return localNode;
    }

    public void setLocalNode(String localNode) {
        this.localNode = localNode;
    }

    public String getNodeRes1() {
        return nodeRes1;
    }

    public void setNodeRes1(String nodeRes1) {
        this.nodeRes1 = nodeRes1;
    }

    public String getNodeRes2() {
        return nodeRes2;
    }

    public void setNodeRes2(String nodeRes2) {
        this.nodeRes2 = nodeRes2;
    }

    public String getNodeRes3() {
        return nodeRes3;
    }

    public void setNodeRes3(String nodeRes3) {
        this.nodeRes3 = nodeRes3;
    }
}
