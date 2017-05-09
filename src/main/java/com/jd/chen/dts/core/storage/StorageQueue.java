package com.jd.chen.dts.core.storage;

import com.jd.chen.dts.core.thread.ILine;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by chenxiaolei3 on 2017/4/19.
 */
public abstract class StorageQueue implements Serializable {
    private static final long serialVersionUID = -855614444596237418L;

    public abstract boolean push(ILine line, long timeout, TimeUnit unit) throws InterruptedException;

    public abstract boolean push(ILine[] lines, int size, long timeout, TimeUnit unit) throws InterruptedException;

    public abstract ILine pull(long timeout, TimeUnit unit) throws InterruptedException;

    public abstract int pull(ILine[] ea, long timeout, TimeUnit unit) throws InterruptedException;

    public abstract void close();

    public abstract int size();

    public abstract int getLineLimit();

    public abstract String info();
}
