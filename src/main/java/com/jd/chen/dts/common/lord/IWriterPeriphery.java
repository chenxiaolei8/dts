package com.jd.chen.dts.common.lord;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public interface IWriterPeriphery extends ITransmissionPeriphery {
    void rollback(IParam param);
}
