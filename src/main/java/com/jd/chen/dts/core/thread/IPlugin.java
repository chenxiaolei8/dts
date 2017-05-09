package com.jd.chen.dts.core.thread;

import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.IPluginMonitor;

import java.util.Map;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public interface IPlugin {
    void setParam(IParam param);

    IParam getParam();

    void setMonitor(IPluginMonitor monitor);

    IPluginMonitor getMonitor();

    void init();

    void connection();

    void finish();

    Map<String, String> getMonitorInfo();
}
