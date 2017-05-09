package com.jd.chen.dts.core.god;

import com.jd.chen.dts.common.config.PluginConfParamKey;
import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.exception.DTSException;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.core.entity.ParamKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class PluginManager {
    private static Log log = LogFactory.getLog(PluginManager.class);
    private static final String PARAM_KEY_CURRENCY = "concurrency";
    private final static String DTS_CONNECT_FILE = "DTS_CONNECT_FILE";

    /**
     * 获取当前任务的 读和写的线程数量
     *
     * @param jobParams
     * @param pluginParams
     * @return
     */
    protected int getConcurrency(IParam jobParams, IParam pluginParams) {
        int concurrency = jobParams.getIntValue(PARAM_KEY_CURRENCY, 1);
        // 获取最大线程并发数
        int maxThreadNum = pluginParams.getIntValue(PluginConfParamKey.MAX_THREAD_NUMBER);
        if (concurrency <= 0 || concurrency > maxThreadNum) {
            log.info("concurrency in conf:" + concurrency + " is invalid!");
            concurrency = 1;
        }

        return concurrency;
    }

    /**
     * 创建读和写的线程池
     *
     * @param concurrency
     * @return
     */
    protected ExecutorService createThreadPool(int concurrency) {
        ThreadPoolExecutor tp = new ThreadPoolExecutor(concurrency, concurrency, 1L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        tp.prestartCoreThread();
        return tp;
    }

    public static void regDataSourceProp(IParam param) {
        String fileName = System.getenv(DTS_CONNECT_FILE);
        String connectProps = param.getValue(ParamKey.connectProps, null);
        if (fileName != null && connectProps != null) {
            Properties props = new Properties();
            try {
                props.load(new FileInputStream(fileName));
                param.putValue(ParamKey.ip, props.getProperty(connectProps + "." + ParamKey.ip).trim());
                param.putValue(ParamKey.port, props.getProperty(connectProps + "." + ParamKey.port).trim());
                param.putValue(ParamKey.username, props.getProperty(connectProps + "." + ParamKey.username).trim());
                param.putValue(ParamKey.password, props.getProperty(connectProps + "." + ParamKey.password).trim());
                param.putValue(ParamKey.dbname, props.getProperty(connectProps + "." + ParamKey.dbname).trim());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new DTSException(e, JobStatus.CONF_FAILED.getStatus());
            }
        }
    }

    public boolean isSuccess(List<Future<Integer>> threadResultList, ExecutorService threadPool) {
        return getStatus(threadResultList, threadPool) == JobStatus.SUCCESS.getStatus();
    }

    protected int getStatus(List<Future<Integer>> threadResultList, ExecutorService threadPool) {
        for (Future<Integer> r : threadResultList) {
            try {
                Integer result = r.get(1, TimeUnit.MICROSECONDS);
                if (result == null || result != JobStatus.SUCCESS.getStatus()) {
                    if (threadPool != null) {
                        //if one thread failed, stop all other threads in the thread pool
                        threadPool.shutdownNow();
                    }
                    return result;
                }
            } catch (TimeoutException e) {
                log.debug("thread is not finished yet");
                continue;
            } catch (InterruptedException e) {
                log.error("Interrupted Exception occurs when getting thread result!");
                continue;
            } catch (ExecutionException e) {
                threadPool.shutdownNow();
                log.error("Execution Exception occurs when getting thread result, this should never happen!", e);
                return JobStatus.FAILED.getStatus();
            }
        }
        return JobStatus.SUCCESS.getStatus();
    }

    public static void main(String[] args) {
        String fileName = System.getenv("JAVA_HOME");
        System.out.println(fileName);
    }
}
