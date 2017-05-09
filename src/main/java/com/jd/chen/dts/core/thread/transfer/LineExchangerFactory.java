package com.jd.chen.dts.core.thread.transfer;

import com.jd.chen.dts.common.lord.IStorage;
import com.jd.chen.dts.core.thread.ILineReceiver;
import com.jd.chen.dts.core.thread.ILineSender;
import com.jd.chen.dts.core.thread.impl.BufferedLineExchanger;

import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public class LineExchangerFactory {

    public static ILineSender createNewLineSender(IStorage storageForRead, List<IStorage> storageForWrite) {
        return new BufferedLineExchanger(storageForRead, storageForWrite);
    }

    public static ILineReceiver createNewLineReceiver(IStorage storageForRead, List<IStorage> storageForWrite) {

        return new BufferedLineExchanger(storageForRead, storageForWrite);
    }
}
