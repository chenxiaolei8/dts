package com.jd.chen.dts.common.lord;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public interface ITransmissionPeriphery {
    void prepare(IParam param, ISourceCounter counter);

    void doPost(IParam param, ITargetCounter counter);
}
