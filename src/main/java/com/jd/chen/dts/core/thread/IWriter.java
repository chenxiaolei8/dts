package com.jd.chen.dts.core.thread;

/**
 * Created by chenxiaolei3 on 2017/4/19.
 */
public interface IWriter extends IPlugin {
    void write(ILineReceiver lineReceiver);

    void commit();
}
