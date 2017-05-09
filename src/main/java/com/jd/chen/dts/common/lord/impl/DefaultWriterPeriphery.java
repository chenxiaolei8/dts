package com.jd.chen.dts.common.lord.impl;

import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.ISourceCounter;
import com.jd.chen.dts.common.lord.ITargetCounter;
import com.jd.chen.dts.common.lord.IWriterPeriphery;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public class DefaultWriterPeriphery implements IWriterPeriphery {
    public void prepare(IParam param, ISourceCounter counter) {
        // 默认预操作
    }

    public void doPost(IParam param, ITargetCounter counter) {
        // do nothing

    }

    public void rollback(IParam param) {
        // do nothing
    }
}
