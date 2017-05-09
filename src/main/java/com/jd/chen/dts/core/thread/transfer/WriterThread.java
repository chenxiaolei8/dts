package com.jd.chen.dts.core.thread.transfer;

import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.exception.DTSException;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.IPluginMonitor;
import com.jd.chen.dts.common.utils.JarLoader;
import com.jd.chen.dts.common.utils.ReflectionUtil;
import com.jd.chen.dts.core.thread.ILineReceiver;
import com.jd.chen.dts.core.thread.IWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Callable;

/**
 * Created by chenxiaolei3 on 2017/4/19.
 */
public class WriterThread implements Callable<Integer> {
    private static Log log = LogFactory.getLog(WriterThread.class);
    private IWriter writer;
    private ILineReceiver lineReceiver;

    public static WriterThread getInstance(ILineReceiver lineReceiver, IParam param, String writerClassName,
                                           String writerPath, IPluginMonitor pm) {
        try {
            IWriter writer = ReflectionUtil.createInstanceByDefaultConstructor(
                    writerClassName, IWriter.class,
                    JarLoader.getInstance(new String[]{writerPath}));
            writer.setParam(param);
            writer.setMonitor(pm);
            return new WriterThread(lineReceiver, writer);
        } catch (Exception e) {
            log.error("Error to create WriterThread: ", e);
            return null;
        }

    }
    private WriterThread(ILineReceiver lineReceiver, IWriter writer) {
        super();
        this.lineReceiver = lineReceiver;
        this.writer = writer;
    }

    public Integer call() throws Exception {
        try{
            writer.init();
            writer.connection();
            writer.write(lineReceiver);
            writer.commit();
            writer.finish();
            return JobStatus.SUCCESS.getStatus();
        } catch(DTSException e){
            log.error("Exception occurs in writer thread!", e);
            return e.getStatusCode();
        } catch(Exception e){
            log.error("Exception occurs in writer thread!", e);
            return JobStatus.FAILED.getStatus();
        }
    }

}
