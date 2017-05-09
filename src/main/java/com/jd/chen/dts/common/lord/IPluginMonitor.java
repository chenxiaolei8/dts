package com.jd.chen.dts.common.lord;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public interface IPluginMonitor {
    /**
     * 获取当前成功数量
     *
     * @return
     */
    long getSuccessLines();

    long getFailedLines();

    void increaseSuccessLines();

    void increaseSuccessLine(long lines);

    void increaseFailedLines();

    void increaseFailedLines(long lines);
}
