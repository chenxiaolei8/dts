package com.jd.chen.dts.core.thread;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public interface IReader extends IPlugin {
    void read(ILineSender lineSender);
}
