package com.jd.chen.dts.core.storage;

import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.exception.DTSException;
import com.jd.chen.dts.core.thread.ILine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * com.jd.chen.dts.core.storage.RAMStorage
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class RAMStorage extends AbstractStorage {
    private static Log log = LogFactory.getLog(RAMStorage.class);
    private StorageQueue sq = null;

    private int waitTime = 3000;


    public boolean init(String id, int lineLimit, int byteLimit, int destructLimit, int waitTime) {
        if (this.getStat() == null) {
            this.setStat(new Statistics(id, this));
        }
        getStat().periodPass();
        if (lineLimit <= 0 || byteLimit <= 0) {
            log.error("Error: line or byte limit is less than or equal to 0.");
            return false;
        }
        setPushClosed(false);
        this.waitTime = waitTime;
        this.setDestructLimit(destructLimit);
        this.sq = new DoubleQueue(lineLimit, byteLimit);
        // this.mars = new SingleQueue(lineLimit,byteLimit);
        return true;
    }

    public boolean push(ILine line) {
        if (getPushClosed()) {
            return false;
        }
        try {
            while (!sq.push(line, waitTime, TimeUnit.MILLISECONDS)) {
                getStat().incLineRRefused(1);
                if (getDestructLimit() > 0
                        && getStat().getLineRRefused() >= getDestructLimit()) {
                    if (getPushClosed()) {
                        log.warn("Close RAMStorage for " + getStat().getId()
                                + ". Queue:" + info() + " Timeout times:"
                                + getStat().getLineRRefused());
                        setPushClosed(true);
                    }
                    throw new DTSException("",
                            JobStatus.WRITE_OUT_OF_TIME.getStatus());
                }
            }
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    public boolean push(ILine[] lines, int size) {
        if (getPushClosed()) {
            return false;
        }

        try {
            while (!sq.push(lines, size, waitTime, TimeUnit.MILLISECONDS)) {
                getStat().incLineRRefused(1);
                if (getDestructLimit() > 0
                        && getStat().getLineRRefused() >= getDestructLimit()) {
                    if (!getPushClosed()) {
                        log.warn("Close RAMStorage for " + getStat().getId()
                                + ". Queue:" + info() + " Timeout times:"
                                + getStat().getLineRRefused());
                        setPushClosed(true);
                    }
                    throw new DTSException("",
                            JobStatus.WRITE_OUT_OF_TIME.getStatus());
                }
            }
        } catch (InterruptedException e) {
            return false;
        }

        getStat().incLineRx(size);
        for (int i = 0; i < size; i++) {
            getStat().incByteRx(lines[i].length());
        }

        return true;
    }

    public ILine pull() {
        ILine line = null;
        try {
            while ((line = sq.pull(waitTime, TimeUnit.MILLISECONDS)) == null) {
                getStat().incLineTRefused(1);
            }
        } catch (InterruptedException e) {
            return null;
        }
        if (line != null) {
            getStat().incLineTx(1);
            getStat().incByteTx(line.length());
        }
        return line;
    }

    public int pull(ILine[] lines) {
        int readNum = 0;
        try {
            while ((readNum = sq.pull(lines, waitTime, TimeUnit.MILLISECONDS)) == 0) {
                getStat().incLineTRefused(1);
                if (getDestructLimit() > 0
                        && getStat().getLineTRefused() >= getDestructLimit()) {
                    if (!getPushClosed()) {
                        log.warn("Close RAMStorage for " + getStat().getId()
                                + ". Queue:" + info() + " Timeout times:"
                                + getStat().getLineRRefused());
                        setPushClosed(true);
                    }
                    throw new DTSException("",
                            JobStatus.READ_OUT_OF_TIME.getStatus());
                }
            }
        } catch (InterruptedException e) {
            return 0;
        }
        if (readNum > 0) {
            getStat().incLineTx(readNum);
            for (int i = 0; i < readNum; i++) {
                getStat().incByteTx(lines[i].length());
            }
        }
        if (readNum == -1) {
            return 0;
        }
        return readNum;
    }

    public void close() {
        setPushClosed(true);
        sq.close();
    }

    public int size() {
        return sq.size();
    }

    public boolean empty() {
        return (size() <= 0);
    }

    public String info() {
        return sq.info();
    }

    public int getLineLimit() {
        return sq.getLineLimit();
    }
}
