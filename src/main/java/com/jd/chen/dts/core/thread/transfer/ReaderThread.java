package com.jd.chen.dts.core.thread.transfer;

import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.exception.DTSException;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.IPluginMonitor;
import com.jd.chen.dts.common.utils.JarLoader;
import com.jd.chen.dts.common.utils.ReflectionUtil;
import com.jd.chen.dts.core.thread.ILineSender;
import com.jd.chen.dts.core.thread.IReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Callable;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public class ReaderThread implements Callable<Integer> {
    private static Log log = LogFactory.getLog(ReaderThread.class);
    private IReader reader;
    private ILineSender lineSender;

    public static ReaderThread getInstance(ILineSender lineSender, IParam param, String readerClassName,
                                           String readerPath, IPluginMonitor monitor) {
        try {
            IReader reader = ReflectionUtil.createInstanceByDefaultConstructor(
                    readerClassName, IReader.class, JarLoader.getInstance(readerPath));
            reader.setParam(param);
            reader.setMonitor(monitor);
            return new ReaderThread(lineSender, reader);
        } catch (Exception e) {
            log.error("Error to create Reader Thread!", e);
            return null;
        }
    }

    private ReaderThread(ILineSender lineSender, IReader reader) {
        super();
        this.lineSender = lineSender;
        this.reader = reader;
    }

    public Integer call() throws Exception {
        try {
            reader.init();
            reader.connection();
            reader.read(lineSender);
            reader.finish();
            return JobStatus.SUCCESS.getStatus();
        } catch (DTSException e) {
            log.error("Exception occurs in reader thread!", e);
            return e.getStatusCode();
        } catch (Exception e) {
            log.error("Exception occurs in reader thread!", e);
            return JobStatus.FAILED.getStatus();
        }
    }
}
