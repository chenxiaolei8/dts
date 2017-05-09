package com.jd.chen.dts.core.thread.impl;

import com.jd.chen.dts.common.lord.IStorage;
import com.jd.chen.dts.core.thread.ILine;
import com.jd.chen.dts.core.thread.ILineReceiver;
import com.jd.chen.dts.core.thread.ILineSender;

import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public class BufferedLineExchanger implements ILineSender, ILineReceiver {
    static private final int DEFAUTL_BUFF_SIZE = 64;
    private ILine[] writeBuf;

    private ILine[] readBuf;

    private int writeBufIdx = 0;

    private int readBufIdx = 0;

    private List<IStorage> storageForWrite;

    private IStorage storageForRead;

    public BufferedLineExchanger(IStorage storageForRead, List<IStorage> storageForWrite) {
        this(storageForRead, storageForWrite, DEFAUTL_BUFF_SIZE);
    }

    public BufferedLineExchanger(IStorage storageForRead,
                                 List<IStorage> storageForWrite, int bufSize) {
        this.storageForRead = storageForRead;
        this.storageForWrite = storageForWrite;
        this.writeBuf = new ILine[bufSize];
        this.readBuf = new ILine[bufSize];
    }


    public ILine receive() {
        if (readBufIdx == 0) {
            readBufIdx = storageForRead.pull(readBuf);
            if (readBufIdx == 0) {
                return null;
            }
        }
        return readBuf[--readBufIdx];
    }

    public ILine createNewLine() {
        return new DefaultLine();
    }

    public Boolean send(ILine line) {
        boolean result = true;
        if (writeBufIdx >= writeBuf.length) {
            if (!writeAllStorage(writeBuf, writeBufIdx)) {
                result = false;
            }
            writeBufIdx = 0;
        }
        writeBuf[writeBufIdx++] = line;
        return result;
    }

    public void flush() {
        if (writeBufIdx > 0) {
            writeAllStorage(writeBuf, writeBufIdx);
        }
        writeBufIdx = 0;
    }

    private boolean writeAllStorage(ILine[] lines, int size) {
        boolean result = true;
        for (IStorage s : this.storageForWrite) {
            if (!s.push(lines, size)) {
                result = false;
            }
        }
        return result;
    }
}
