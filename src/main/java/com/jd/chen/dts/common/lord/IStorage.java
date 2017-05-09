package com.jd.chen.dts.common.lord;

import com.jd.chen.dts.core.storage.Statistics;
import com.jd.chen.dts.core.thread.ILine;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public interface IStorage {
    /**
     * 初始化 最重要的类
     *
     * @param id
     * @param lineLimit
     * @param byteLimit
     * @param destructLimit
     * @param waitTime
     * @return
     */
    boolean init(String id, int lineLimit, int byteLimit, int destructLimit, int waitTime);

    /**
     * push 一行数据
     *
     * @param line
     * @return
     */
    boolean push(ILine line);

    /**
     * push 多行数据
     *
     * @param lines
     * @param size
     * @return
     */
    boolean push(ILine[] lines, int size);

    /**
     * pull 一行数据
     *
     * @return
     */
    ILine pull();

    /**
     * pull 多行
     *
     * @param lines
     * @return
     */
    int pull(ILine[] lines);

    /**
     * close
     */
    void close();

    /**
     * 获取当前行大小
     *
     * @return
     */
    int size();

    /**
     * 获取当前是否为空
     *
     * @return
     */
    boolean empty();

    /**
     * 获取当前存储信息
     *
     * @return
     */
    String info();

    //全局统计信息
    Statistics getStat();

}
