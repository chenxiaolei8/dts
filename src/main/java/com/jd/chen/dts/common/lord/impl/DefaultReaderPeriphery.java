package com.jd.chen.dts.common.lord.impl;

import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.IReaderPeriphery;
import com.jd.chen.dts.common.lord.ISourceCounter;
import com.jd.chen.dts.common.lord.ITargetCounter;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public class DefaultReaderPeriphery implements IReaderPeriphery {
    /**
     * 预处理
     * @param param
     * @param counter
     */
    public void prepare(IParam param, ISourceCounter counter) {
        // 啥也不做
    }

    /**
     * 后处理
     * @param param
     * @param counter
     */
    public void doPost(IParam param, ITargetCounter counter) {
        // 啥也不做
    }
}
