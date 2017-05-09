package com.jd.chen.dts.core.thread.impl;

import com.jd.chen.dts.core.thread.ILine;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public class DefaultLine implements ILine {
    private String[] fieldList;
    private int length;
    private int fieldNum;
    public static final int LINE_MAX_FIELD = 1024;

    public DefaultLine() {
        this.fieldList = new String[LINE_MAX_FIELD];
    }

    public void clear() {
        length = 0;
        fieldNum = 0;
    }

    public int length() {
        return length;
    }

    public boolean addField(String field) {
        fieldList[fieldNum] = field;
        fieldNum++;
        if (field != null) {
            length += field.length();
        }
        return true;
    }

    public boolean addField(String field, int index) {
        fieldList[index] = field;
        if (fieldNum < index + 1) {
            fieldNum = index + 1;
        }
        if (field != null) {
            length += field.length();
        }
        return true;
    }

    public int getFieldNum() {
        return fieldNum;
    }

    public String getField(int idx) {
        return fieldList[idx];
    }

    public String checkAndGetField(int idx) {
        if (idx < 0 || idx >= fieldNum) {
            return null;
        }
        return fieldList[idx];
    }

    public StringBuffer toStringBuffer(char separator) {
        StringBuffer tmp = new StringBuffer();
        tmp.append(fieldNum);
        tmp.append(":");
        for (int i = 0; i < fieldNum; i++) {
            tmp.append(fieldList[i]);
            if (i < fieldNum - 1) {
                tmp.append(separator);
            }
        }
        return tmp;
    }

    public String toString(char separator) {
        return this.toStringBuffer(separator).toString();
    }

    public ILine fromString(String lineStr, char separator) {
        return null;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(fieldList);
        result = prime * result + fieldNum;
        result = prime * result + length;
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DefaultLine other = (DefaultLine) obj;
        if (!Arrays.equals(fieldList, other.fieldList))
            return false;
        if (fieldNum != other.fieldNum)
            return false;
        if (length != other.length)
            return false;
        return true;
    }

    public static void main(String[] args) {
//        ILine line = new DefaultLine();
//        line.addField("a");
//        line.addField("b");
//        StringBuffer stringBuffer = line.toStringBuffer('\t');
//        System.out.println(stringBuffer.toString());
    }
}
