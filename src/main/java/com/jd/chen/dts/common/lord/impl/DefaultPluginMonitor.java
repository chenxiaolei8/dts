package com.jd.chen.dts.common.lord.impl;

import com.jd.chen.dts.common.lord.IPluginMonitor;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class DefaultPluginMonitor implements IPluginMonitor {
    private long successLines;
    private long failedLines;

    public long getSuccessLines() {
        return successLines;
    }

    public long getFailedLines() {
        return failedLines;
    }

    public void increaseSuccessLines() {
        increaseSuccessLine(1);
    }

    public void increaseSuccessLine(long lines) {
        successLines += lines;
    }

    public void increaseFailedLines() {
        increaseFailedLines(1);
    }

    public void increaseFailedLines(long lines) {
        failedLines += lines;
    }
}
