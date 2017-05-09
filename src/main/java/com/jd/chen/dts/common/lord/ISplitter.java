package com.jd.chen.dts.common.lord;

import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public interface ISplitter {
    void init(IParam jobParams);

    List<IParam> split();
}
