package com.jd.chen.dts.core.thread;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public interface ILineSender {
    ILine createNewLine();

    Boolean send(ILine line);

    void flush();
}
