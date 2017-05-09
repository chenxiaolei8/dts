package com.jd.chen.dts.common.lord.impl;

import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.ISplitter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public abstract class AbstractSplitter implements ISplitter {
    protected IParam param;

    public void init(IParam jobParams) {
        param = jobParams;
    }

    public List<IParam> split() {
        List<IParam> result = new ArrayList<IParam>();
        result.add(param);
        return result;
    }
}
