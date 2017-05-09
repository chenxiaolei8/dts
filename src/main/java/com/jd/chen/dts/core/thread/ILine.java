package com.jd.chen.dts.core.thread;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public interface ILine {
    /**
     * 添加字段
     *
     * @param field
     * @return
     */
    boolean addField(String field);

    /**
     * 添加指定位置的属性
     *
     * @param field
     * @param index
     * @return
     */
    boolean addField(String field, int index);

    /**
     * 注意界限
     *
     * @param idx
     * @return
     */
    String getField(int idx);

    /**
     * @param idx
     * @return
     */
    String checkAndGetField(int idx);

    /**
     * @return
     */
    int getFieldNum();

    /**
     * @param separator
     * @return
     */
    StringBuffer toStringBuffer(char separator);

    /**
     * @param separator
     * @return
     */
    String toString(char separator);

    /**
     * @param lineStr
     * @param separator
     * @return
     */
    ILine fromString(String lineStr, char separator);

    /**
     * @return
     */
    int length();
}
