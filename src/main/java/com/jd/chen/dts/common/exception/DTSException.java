package com.jd.chen.dts.common.exception;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class DTSException extends RuntimeException {


    private static final long serialVersionUID = -6912573533046160060L;
    private int statusCode;
    private String pluginID;


    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getPluginID() {
        return pluginID;
    }

    public void setPluginID(String pluginID) {
        this.pluginID = pluginID;
    }

    public DTSException(Exception e, int jobStatus) {
        super(e);
        this.statusCode = jobStatus;
    }

    public DTSException(String m, int jobStatus) {
        super(m);
        this.statusCode = jobStatus;
    }

    public DTSException(Exception e, int jobStatus, String pluginID) {
        super(e);
        this.statusCode = jobStatus;
        this.pluginID = pluginID;
    }

    public DTSException(String m, int jobStatus, String pluginID) {
        super(m);
        this.statusCode = jobStatus;
        this.pluginID = pluginID;
    }

    public DTSException(int jobStatus) {
        this.statusCode = jobStatus;
    }
}
