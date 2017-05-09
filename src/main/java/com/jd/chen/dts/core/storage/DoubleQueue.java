package com.jd.chen.dts.core.storage;

import com.jd.chen.dts.core.thread.ILine;
import com.jd.chen.dts.core.thread.impl.DefaultLine;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by chenxiaolei3 on 2017/4/19.
 */
public class DoubleQueue extends StorageQueue {
    private static final long serialVersionUID = 917779436784117201L;
    private int lineLimit;

    //private int byteLimit;

    //两个队列
    private final ILine[] itemsA;

    private final ILine[] itemsB;

    private ILine[] writeArray, readArray;

    private volatile int writeCount, readCount;

    private int writePosition, readPosition;

    private ReentrantLock readLock, writeLock;

    private Condition notFull;

    private Condition awake;

    private boolean closed = false;

    private int spillSize = 0;

    private long lineRx = 0;

    private long lineTx = 0;

    private long byteRx = 0;

    public String info() {
        return "Read " + lineRx + " | Write " + lineTx + " |";
    }

    public int getLineLimit() {
        return lineLimit;
    }

    public void setLineLimit(int capacity) {
        this.lineLimit = capacity;
    }

    /**
     * 向缓存流插入数据
     *
     * @param line
     */
    private void insert(ILine line) {
        writeArray[writePosition] = line;
        ++writePosition;
        ++writeCount;
        ++lineRx;
        byteRx += line.length();
    }

    private void insert(ILine[] lines, int size) {
        for (int i = 0; i < size; ++i) {
            writeArray[writePosition] = lines[i];
            ++writePosition;
            ++writeCount;
            ++lineRx;
            byteRx += lines[i].length();
        }
    }

    /**
     * 提取记录
     *
     * @return
     */
    private ILine extract() {
        ILine e = readArray[readPosition];
        readArray[readPosition] = null;
        ++readPosition;
        --readCount;
        ++lineTx;
        return e;
    }

    private int extract(ILine[] ea) {
        int readsize = Math.min(ea.length, readCount);
        for (int i = 0; i < readsize; ++i) {
            ea[i] = readArray[readPosition];
            readArray[readPosition] = null;
            ++readPosition;
            --readCount;
            ++lineTx;
        }
        return readsize;
    }

    private long queueSwitch(long timeout, boolean isInfinite)
            throws InterruptedException {
        writeLock.lock();
        try {
            if (writeCount <= 0) {
                if (closed) {
                    return -2;
                }
                try {
                    if (isInfinite && timeout <= 0) {
                        awake.await();
                        return -1;
                    } else {
                        return awake.awaitNanos(timeout);
                    }
                } catch (InterruptedException ie) {
                    awake.signal();
                    throw ie;
                }
            } else {
                ILine[] tmpArray = readArray;
                readArray = writeArray;
                writeArray = tmpArray;

                readCount = writeCount;
                readPosition = 0;

                writeCount = 0;
                writePosition = 0;

                notFull.signal();
                // logger.debug("Queue switch successfully!");
                return -1;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean push(ILine line, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (line == null) {
            throw new NullPointerException();
        }
        long nanoTime = unit.toNanos(timeout);
        writeLock.lockInterruptibly();
        try {
            for (; ; ) {
                if (writeCount < writeArray.length) {
                    insert(line);
                    if (writeCount == 1) {
                        awake.signal();
                    }
                    return true;
                }

                // Time out
                if (nanoTime <= 0) {
                    return false;
                }
                // keep waiting
                try {
                    nanoTime = notFull.awaitNanos(nanoTime);
                } catch (InterruptedException ie) {
                    notFull.signal();
                    throw ie;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean push(ILine[] lines, int size, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (lines == null) {
            throw new NullPointerException();
        }
        long nanoTime = unit.toNanos(timeout);
        writeLock.lockInterruptibly();
        try {
            for (; ; ) {
                if (writeCount + size <= writeArray.length) {
                    insert(lines, size);
                    if (writeCount >= spillSize) {
                        awake.signalAll();
                    }
                    return true;
                }

                // Time out
                if (nanoTime <= 0) {
                    return false;
                }
                // keep waiting
                try {
                    nanoTime = notFull.awaitNanos(nanoTime);
                } catch (InterruptedException ie) {
                    notFull.signal();
                    throw ie;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void close() {
        writeLock.lock();
        try {
            closed = true;
            awake.signalAll();
        } finally {
            writeLock.unlock();
        }
    }

    public ILine pull(long timeout, TimeUnit unit) throws InterruptedException {
        long nanoTime = unit.toNanos(timeout);
        readLock.lockInterruptibly();

        try {
            for (; ; ) {
                if (readCount > 0) {
                    return extract();
                }

                if (nanoTime <= 0) {
                    return null;
                }
                nanoTime = queueSwitch(nanoTime, true);
            }
        } finally {
            readLock.unlock();
        }
    }

    public int pull(ILine[] ea, long timeout, TimeUnit unit)
            throws InterruptedException {
        long nanoTime = unit.toNanos(timeout);
        readLock.lockInterruptibly();

        try {
            for (; ; ) {
                if (readCount > 0) {
                    return extract(ea);
                }

                if (nanoTime == -2) {
                    return -1;
                }

                if (nanoTime <= 0) {
                    return 0;
                }
                nanoTime = queueSwitch(nanoTime, false);
            }
        } finally {
            readLock.unlock();
        }
    }

    public int size() {
        return (writeCount + readCount);
    }


    /**
     * 初始化双端缓存流
     *
     * @param lineLimit
     * @param byteLimit
     */
    public DoubleQueue(int lineLimit, int byteLimit) {
        if (lineLimit <= 0 || byteLimit <= 0) {
            throw new IllegalArgumentException(
                    "Queue initial capacity can't less than 0!");
        }
        this.lineLimit = lineLimit;
//		this.byteLimit = byteLimit;
        itemsA = new ILine[lineLimit];
        itemsB = new ILine[lineLimit];

        readLock = new ReentrantLock();
        writeLock = new ReentrantLock();

        notFull = writeLock.newCondition();
        awake = writeLock.newCondition();

        readArray = itemsA;
        writeArray = itemsB;
        spillSize = lineLimit * 8 / 10;
    }

    public static void main(String[] args) {
        int i = 0;
        // 初始化3000的队列
        DoubleQueue dq = new DoubleQueue(3000, 1);
        int size = 64;

        for (int k = 0; k < 10; k++) {
            PullThread thread = new PullThread(dq, size);
            thread.start();
        }
        ILine[] lines = new ILine[size];
        Date date = new Date();
        while (i < 1280000 / size) { // 20000
            for (int j = 0; j < size; j++) {
                ILine line = new DefaultLine();
                line.addField("1 + ==>" + size);
                line.addField("2 + ==>" + size);
                lines[j] = line;
            }
            try {
                dq.push(lines, size, 3000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }
        System.out.println("所需时间: " + (new Date().getTime() - date.getTime()));

    }

    static class PullThread extends Thread {
        DoubleQueue dq;
        int size;

        public PullThread(DoubleQueue dq, int size) {
            this.dq = dq;
            this.size = size;
        }

        @Override
        public void run() {
            ILine[] lines = new ILine[size];
            try {
                while (true) {
                    if (dq.pull(lines, 100, TimeUnit.MILLISECONDS) <= 0) {
                        System.out.println(lines.length+" === " +lines[0].length());
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
