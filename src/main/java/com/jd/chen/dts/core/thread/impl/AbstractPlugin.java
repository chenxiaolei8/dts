package com.jd.chen.dts.core.thread.impl;

import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.IPluginMonitor;
import com.jd.chen.dts.core.thread.IPlugin;

import java.util.Map;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public abstract class AbstractPlugin implements IPlugin {
    private IParam param;

    private IPluginMonitor monitor;

    private String pluginName;

    private String pluginVersion;

    public static final String PLUGINID = "pluginID";

    public void setParam(IParam param){
        this.param = param;
    }

    public IParam getParam(){
        return param;
    }

    public void setMonitor(IPluginMonitor monitor){
        this.monitor = monitor;
    }

    public IPluginMonitor getMonitor(){
        return monitor;
    }

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public String getPluginVersion() {
        return pluginVersion;
    }

    public void setPluginVersion(String pluginVersion) {
        this.pluginVersion = pluginVersion;
    }

    public void init() {
    }

    public void connection() {
    }

    public void finish() {
    }

    public Map<String, String> getMonitorInfo() {
        return null;
    }
}
