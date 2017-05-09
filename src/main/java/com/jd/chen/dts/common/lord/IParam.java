package com.jd.chen.dts.common.lord;

import java.util.Collection;
import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/13.
 * 封装基本信息 可能会涉及到多个接收端 复制多份数据
 */
public interface IParam extends Cloneable {

    String getValue(String key);

    String getValue(String key, String defaultValue);

    char getCharValue(String key);

    char getCharValue(String key, char defaultValue);

    int getIntValue(String key);

    int getIntValue(String key, int defaultValue);

    double getDoubleValue(String key);

    double getDoubleValue(String key, double defaultValue);

    boolean getBooleanValue(String key);

    boolean getBooleanValue(String key, boolean defaultValue);

    long getLongValue(String key);

    long getLongValue(String key, long defaultValue);

    List<Character> getCharList(String key);

    List<Character> getCharList(String key, List<Character> list);

    void putValue(String key, String value);

    void mergeTo(IParam param);

    void mergeTo(Collection<IParam> paramCollection);

    IParam clone();
}
