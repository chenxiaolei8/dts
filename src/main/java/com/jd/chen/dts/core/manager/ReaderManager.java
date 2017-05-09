package com.jd.chen.dts.core.manager;

import com.jd.chen.dts.common.config.JobPluginConf;
import com.jd.chen.dts.common.config.PluginConfParamKey;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.IPluginMonitor;
import com.jd.chen.dts.common.lord.IReaderPeriphery;
import com.jd.chen.dts.common.lord.ISplitter;
import com.jd.chen.dts.common.lord.impl.DefaultReaderPeriphery;
import com.jd.chen.dts.common.lord.impl.DefaultSplitter;
import com.jd.chen.dts.common.utils.JarLoader;
import com.jd.chen.dts.common.utils.ReflectionUtil;
import com.jd.chen.dts.core.god.PluginManager;
import com.jd.chen.dts.core.storage.StorageManager;
import com.jd.chen.dts.core.thread.transfer.LineExchangerFactory;
import com.jd.chen.dts.core.thread.transfer.ReaderThread;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by chenxiaolei3 on 2017/4/17.
 */
public class ReaderManager extends PluginManager {
    private static Log log = LogFactory.getLog(ReaderManager.class);
    // readerPool
    private ExecutorService readerPool;
    private StorageManager storageManager;
    private MonitorManager monitorManager;
    // 预先的动作和后置的动作
    private IReaderPeriphery readerPeriphery;
    private IParam jobParams;
    private List<Future<Integer>> threadResultList;
    private static ReaderManager rmInstance;
    private static final int TIME_OUT = 10 * 60 * 60;// 60 * ten minutes

    private ReaderManager(StorageManager storageManager,
                          MonitorManager monitorManager) {
        super();
        this.storageManager = storageManager;
        this.monitorManager = monitorManager;
        threadResultList = new ArrayList<Future<Integer>>();
    }

    public static ReaderManager getInstance(StorageManager storageManager,
                                            MonitorManager monitorManager) {
        if (rmInstance == null) {
            rmInstance = new ReaderManager(storageManager, monitorManager);
        }
        return rmInstance;
    }

    /**
     * 做一些准备工作 开启线程池 提交线程
     *
     * @param jobPluginConf  job_reader_conf
     * @param pluginParams 已经存在的reader配置
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void run(JobPluginConf jobPluginConf, IParam pluginParams)
            throws TimeoutException, ExecutionException, InterruptedException {
        jobParams = jobPluginConf.getPluginParam();
        String readerPath = pluginParams.getValue(PluginConfParamKey.PATH);
        //预处理类 不存在 则使用默认的空预处理类 存在 反射生成
        String readerPeripheryClassName = pluginParams
                .getValue(PluginConfParamKey.PERIPHERY_CLASS_NAME);
        if (StringUtils.isEmpty(readerPeripheryClassName)) {
            readerPeriphery = new DefaultReaderPeriphery();
        } else {
            readerPeriphery = ReflectionUtil
                    .createInstanceByDefaultConstructor(
                            readerPeripheryClassName, IReaderPeriphery.class,
                            JarLoader.getInstance(readerPath));
        }
        ReadPrepareCallable<List<IParam>> readCallable = new ReadPrepareCallable<List<IParam>>();
        readCallable.readPeriphery = readerPeriphery;
        readCallable.jobParams = jobParams;
        runWithTimeout(new FutureTask<List<IParam>>(readCallable));

        // splitter 分割
        String splitterClassName = pluginParams
                .getValue(PluginConfParamKey.SPLITTER_CLASS_NAME);
        ISplitter splitter = null;
        if (StringUtils.isEmpty(splitterClassName)) {
            splitter = new DefaultSplitter();
        } else {
            splitter = ReflectionUtil.createInstanceByDefaultConstructor(
                    splitterClassName, ISplitter.class, JarLoader
                            .getInstance(readerPath));
        }
        splitter.init(jobParams);
        ReadSplitCallable<List<IParam>> splitCallable = new ReadSplitCallable<List<IParam>>();
        splitCallable.splitter = splitter;
        List<IParam> splittedParam = (List<IParam>) runWithTimeout(new FutureTask<List<IParam>>(
                splitCallable));

        //最大线程数 最大线程10 出意外情况 默认一个
        int concurrency = getConcurrency(jobParams, pluginParams);
        String readerClassName = pluginParams
                .getValue(PluginConfParamKey.PLUGIN_CLASS_NAME);

        readerPool = createThreadPool(concurrency);
        for (IParam p : splittedParam) {
            IPluginMonitor pm = monitorManager.newReaderMonitor();
            //创建reader线程
            ReaderThread rt = ReaderThread.getInstance(LineExchangerFactory
                            .createNewLineSender(null, storageManager
                                    .getStorageForReader()), p, readerClassName,
                    readerPath, pm);

            Future<Integer> r = readerPool.submit(rt);
            threadResultList.add(r);
        }
        log.info("dts start to read data");
        // Do not accept any new threads
        readerPool.shutdown();
        // 'run' method end!
    }

    /**
     * 预处理
     *
     * @param task
     * @return
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static List<IParam> runWithTimeout(FutureTask<List<IParam>> task)
            throws TimeoutException, ExecutionException, InterruptedException {
        task.run();
        return task.get(TIME_OUT, TimeUnit.SECONDS);
    }

    // 预处理
    class ReadPrepareCallable<V> implements Callable<V> {
        IReaderPeriphery readPeriphery;
        IParam jobParams;

        public V call() throws Exception {
            readPeriphery.prepare(jobParams, monitorManager);
            return null;
        }
    }

    /**
     * @param <V>
     */
    class ReadSplitCallable<V> implements Callable<V> {
        ISplitter splitter;

        @SuppressWarnings("unchecked")
        public V call() throws Exception {
            V splittedParam = (V) splitter.split();
            return (V) splittedParam;
        }
    }

    public boolean terminate() {
        isSuccess();
        if (readerPool.isTerminated()) {
            readerPeriphery.doPost(jobParams, monitorManager);
            return true;
        }
        return false;
    }

    public boolean isSuccess() {
        return isSuccess(threadResultList, readerPool);
    }

    public int getStatus() {
        return getStatus(threadResultList, readerPool);
    }
}
