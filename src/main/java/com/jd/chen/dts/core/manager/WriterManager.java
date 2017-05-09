package com.jd.chen.dts.core.manager;

import com.jd.chen.dts.common.config.JobPluginConf;
import com.jd.chen.dts.common.config.PluginConfParamKey;
import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.IPluginMonitor;
import com.jd.chen.dts.common.lord.ISplitter;
import com.jd.chen.dts.common.lord.IWriterPeriphery;
import com.jd.chen.dts.common.lord.impl.DefaultSplitter;
import com.jd.chen.dts.common.lord.impl.DefaultWriterPeriphery;
import com.jd.chen.dts.common.utils.JarLoader;
import com.jd.chen.dts.common.utils.ReflectionUtil;
import com.jd.chen.dts.core.god.PluginManager;
import com.jd.chen.dts.core.storage.StorageManager;
import com.jd.chen.dts.core.thread.impl.AbstractPlugin;
import com.jd.chen.dts.core.thread.transfer.LineExchangerFactory;
import com.jd.chen.dts.core.thread.transfer.WriterThread;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;

import static com.jd.chen.dts.core.manager.ReaderManager.runWithTimeout;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class WriterManager extends PluginManager {
    private static Log log = LogFactory.getLog(WriterManager.class);
    private static final String JOB_PARAM_FAILED_LINES_THRESHOLD = "failedLinesThreshold";
    private static final int TIME_OUT = 10 * 60 * 60;// 60 * ten minutes

    private Map<String, ExecutorService> writerPoolMap;
    private StorageManager storageManager;
    private MonitorManager monitorManager;
    private Map<String, IWriterPeriphery> writerPeripheryMap;
    private Map<String, IParam> writerToJobParamsMap;
    private Map<String, List<Future<Integer>>> writerToResultsMap;

    private static WriterManager wmInstance;

    private WriterManager(StorageManager storageManager,
                          MonitorManager monitorManager, int writerNum) {
        super();
        writerPoolMap = new HashMap<String, ExecutorService>(writerNum);
        writerPeripheryMap = new HashMap<String, IWriterPeriphery>(writerNum);
        writerToJobParamsMap = new HashMap<String, IParam>(writerNum);
        writerToResultsMap = new HashMap<String, List<Future<Integer>>>(
                writerNum);
        this.storageManager = storageManager;
        this.monitorManager = monitorManager;
    }

    /**
     * 初始化wm
     *
     * @param storageManager
     * @param monitorManager
     * @param writerNum
     * @return
     */
    public static WriterManager getInstance(StorageManager storageManager,
                                            MonitorManager monitorManager, int writerNum) {
        if (wmInstance == null) {
            wmInstance = new WriterManager(storageManager, monitorManager,
                    writerNum);
        }

        return wmInstance;
    }

    /**
     * 允许最大失败量
     *
     * @param writerID
     * @return
     */
    public int getFailedLinesThreshold(String writerID) {
        IParam jobParam = writerToJobParamsMap.get(writerID);
        if (jobParam == null) {
            return 0;
        }
        return jobParam.getIntValue(JOB_PARAM_FAILED_LINES_THRESHOLD, 0);
    }

    public void run(List<JobPluginConf> jobPluginList,
                    Map<String, IParam> pluginParamsMap) throws TimeoutException,
            ExecutionException, InterruptedException {
        // 可能多个输出端
        for (JobPluginConf jobPluginConf : jobPluginList) {

            String writerID = jobPluginConf.getId();
            IParam jobParams = jobPluginConf.getPluginParam();
            writerToJobParamsMap.put(writerID, jobParams);
            IParam pluginParams = pluginParamsMap.get(jobPluginConf
                    .getPluginName());
            jobParams.putValue(AbstractPlugin.PLUGINID, writerID);
            String writerPath = pluginParams.getValue(PluginConfParamKey.PATH);
            String writerPeripheryClassName = pluginParams
                    .getValue(PluginConfParamKey.PERIPHERY_CLASS_NAME);
            IWriterPeriphery writerPeriphery = null;
            //接着写写入
            if (StringUtils.isEmpty(writerPeripheryClassName)) {
                writerPeriphery = new DefaultWriterPeriphery();
            } else {
                writerPeriphery = ReflectionUtil
                        .createInstanceByDefaultConstructor(
                                writerPeripheryClassName,
                                IWriterPeriphery.class,
                                JarLoader.getInstance(writerPath));
            }
            writerPeripheryMap.put(writerID, writerPeriphery);
            //默认分割
            String splitterClassName = pluginParams
                    .getValue(PluginConfParamKey.SPLITTER_CLASS_NAME);
            ISplitter splitter = null;
            if (StringUtils.isEmpty(splitterClassName)) {
                splitter = new DefaultSplitter();
            } else {
                splitter = ReflectionUtil.createInstanceByDefaultConstructor(
                        splitterClassName, ISplitter.class,
                        JarLoader.getInstance(writerPath));
            }

            WritePrepareCallable<List<IParam>> writerCallable = new WritePrepareCallable<List<IParam>>();
            writerCallable.writerPeriphery = writerPeriphery;
            writerCallable.jobParams = jobParams;
            runWithTimeout(new FutureTask<List<IParam>>(writerCallable));
            splitter.init(jobParams);

            WriteSplitCallable<List<IParam>> splitCallable = new WriteSplitCallable<List<IParam>>();
            splitCallable.splitter = splitter;
            List<IParam> splittedParam = (List<IParam>) runWithTimeout(new FutureTask<List<IParam>>(
                    splitCallable));

            int concurrency = getConcurrency(jobParams, pluginParams);
            String writeClassName = pluginParams
                    .getValue(PluginConfParamKey.PLUGIN_CLASS_NAME);
            ExecutorService writerPool = createThreadPool(concurrency);
            writerPoolMap.put(writerID, writerPool);

            List<Future<Integer>> resultList = new ArrayList<Future<Integer>>();
            for (IParam p : splittedParam) {
                IPluginMonitor pm = monitorManager.newWriterMonitor(writerID);
                WriterThread rt = WriterThread.getInstance(LineExchangerFactory
                        .createNewLineReceiver(
                                storageManager.getStorageForWriter(writerID),
                                null), p, writeClassName, writerPath, pm);

                Future<Integer> r = writerPool.submit(rt);
                resultList.add(r);
            }
            writerToResultsMap.put(writerID, resultList);
            log.info("Writer: " + writerID + " start to write data");
            // Do not accept any new threads
            writerPool.shutdown();

        }
    }


    class WritePrepareCallable<V> implements Callable<V> {
        IWriterPeriphery writerPeriphery;
        IParam jobParams;

        public V call() throws Exception {
            writerPeriphery.prepare(jobParams, monitorManager);
            return null;
        }
    }

    class WriteSplitCallable<V> implements Callable<V> {
        ISplitter splitter;

        @SuppressWarnings("unchecked")
        public V call() throws Exception {
            List<IParam> splittedParam = splitter.split();
            return (V) splittedParam;

        }
    }

    public Set<String> getFailedWriterID() {
        Set<String> failedIDs = new HashSet<String>();
        for (String writerID : writerToResultsMap.keySet()) {
            if (!isSuccess(writerToResultsMap.get(writerID),
                    writerPoolMap.get(writerID))) {
                failedIDs.add(writerID);
            }
        }
        return failedIDs;
    }

    private void terminate(String writerID) {
        IWriterPeriphery writerPeriphery = writerPeripheryMap.get(writerID);
        if (writerPeriphery == null) {
            log.error("can not find any writer periphery for " + writerID);
            return;
        }
        IParam jobParams = writerToJobParamsMap.get(writerID);
        if (jobParams == null) {
            log.error("can not find any job parameters for " + writerID);
            return;
        }

        writerPeriphery.doPost(jobParams, monitorManager);
    }

    public boolean terminate(boolean writerConsistency) {
        boolean result = true;
        Set<String> failedIDs = getFailedWriterID();
        if (writerConsistency && failedIDs.size() > 0) {
            for (String writerID : writerPoolMap.keySet()) {
                ExecutorService es = writerPoolMap.get(writerID);
                es.shutdownNow();
                terminate(writerID);
            }
            writerPoolMap.clear();
            return true;
        }
        Collection<String> needToRemoveWriterIDList = new ArrayList<String>(
                writerPoolMap.size());
        for (String writerID : writerPoolMap.keySet()) {
            ExecutorService es = writerPoolMap.get(writerID);
            if (es.isTerminated()) {
                terminate(writerID);
                needToRemoveWriterIDList.add(writerID);
            } else {
                result = false;
            }
        }

        // remove terminated writers from writerPoolMap
        for (String writerID : needToRemoveWriterIDList) {
            writerPoolMap.remove(writerID);
        }

        return result;
    }

    public int getErrorCode() {
        int result = Integer.MAX_VALUE;
        for (String writerID : writerToResultsMap.keySet()) {
            int status = getStatus(writerToResultsMap.get(writerID),
                    writerPoolMap.get(writerID));
            if (status != JobStatus.SUCCESS.getStatus() && status < result) {
                result = status;
            }
        }
        return result;
    }

    public void rollbackAll() {
        for (String writerID : writerPeripheryMap.keySet()) {
            rollback(writerID);
        }
    }

    public void rollback(Collection<String> writerIDs) {
        for (String writerID : writerIDs) {
            rollback(writerID);
        }
    }

    public void rollback(String writerID) {
        IWriterPeriphery writerPeriphery = writerPeripheryMap.get(writerID);
        if (writerPeriphery == null) {
            log.error("can not find any writer periphery for " + writerID);
            return;
        }
        IParam jobParams = writerToJobParamsMap.get(writerID);
        if (jobParams == null) {
            log.error("can not find any job parameters for " + writerID);
            return;
        }

        writerPeriphery.rollback(jobParams);
    }

    public void killAll() {
        for (String writerID : writerPoolMap.keySet()) {
            ExecutorService es = writerPoolMap.get(writerID);
            es.shutdownNow();
        }
    }


}
