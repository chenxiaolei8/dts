package com.jd.chen.dts.core;

import com.jd.chen.dts.common.config.EngineConfParamKey;
import com.jd.chen.dts.common.config.JobConf;
import com.jd.chen.dts.common.config.JobPluginConf;
import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.exception.DTSException;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.utils.JobDBUtil;
import com.jd.chen.dts.common.utils.ParseXMLUtil;
import com.jd.chen.dts.core.entity.DTSJobInfo;
import com.jd.chen.dts.core.storage.StorageConf;
import com.jd.chen.dts.core.god.PluginManager;
import com.jd.chen.dts.core.manager.FailedInfo;
import com.jd.chen.dts.core.manager.MonitorManager;
import com.jd.chen.dts.core.manager.ReaderManager;
import com.jd.chen.dts.core.storage.StorageManager;
import com.jd.chen.dts.core.manager.WriterManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Created by chenxiaolei3 on 2017/4/13.
 */
public class Engine {
    private static Log log = LogFactory.getLog(Engine.class);
    private static final String DEFAULT_STORAGE_CLASS_NAME = "com.jd.chen.dts.core.storage.RAMStorage";
    private static final int STATUS_CHECK_INTERVAL = 1000;
    private static final int INFO_SHOW_PERIOD = 10;
    private static final int DEFAULT_STORAGE_LINE_LIMIT = 3000;
    private static final int DEFAULT_STORAGE_BYTE_LIMIT = 1000000;
    private static final int DEFAULT_STORAGE_DESTRUCT_LIMIT = 1;
    private static final int DEFAULT_STORAGE_WAIT_TIME = 20000;
    private static final String IP = "ip";
    private static final String DB = "dbname";
    private static final String TABLE = "tableName";
    private boolean writerConsistency = false;

    private IParam engineConf;
    private Map<String, IParam> pluginReg;

    public Engine(IParam engineConf, Map<String, IParam> pluginReg) {
        this.engineConf = engineConf;
        this.pluginReg = pluginReg;
    }

    /**
     * 所有任务的入口 初始化线程 验证参数等
     *
     * @return
     */
    public int startWork(JobConf jobConf) {
        Date now = new Date();
        long time = 0;
        MonitorManager monitorManager = null;
        // 数据发射源
        String source = null;
        // 数据接收源
        String target = null;
        // 定义任务起始状态
        JobStatus status = JobStatus.RUNNING;
        // 获取当前任务状态
        int statusCode = status.getStatus();
        // 创建空写管理
        WriterManager writerManager = null;
        try {
            log.info("dts Start!");
            // 配置storageManager 以及 monitorManager
            List<StorageConf> storageConfList = createStorageConfs(jobConf);
            if (storageConfList == null || storageConfList.isEmpty()) {
                log.error("No writer is defined in job configuration or there are some errors in writer configuration");
                return JobStatus.FAILED.getStatus();
            }
            // 获取写入端个数 可能多个不同的写入端 & 开启监控
            int writerNum = jobConf.getWriterNum();
            StorageManager storageManager = new StorageManager(storageConfList);
            monitorManager = new MonitorManager(writerNum);
            monitorManager.setStorageManager(storageManager);

            // 获取读写 & 开启读写
            JobPluginConf readerConf = jobConf.getReaderConf();
            List<JobPluginConf> writerConf = jobConf.getWriterConfs();
            PluginManager.regDataSourceProp(readerConf.getPluginParam());
            // 将源与目标 各自组装成字符 结束后 要存到mysql  ip/db/table
            source = readerConf.getPluginName() + "/"
                    + readerConf.getPluginParam().getValue(IP, "IP_UNKNOWN")
                    + "/" + readerConf.getPluginParam().getValue(DB, "") + "/"
                    + readerConf.getPluginParam().getValue(TABLE, "");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < writerConf.size(); i++) {
                JobPluginConf conf = writerConf.get(i);
                PluginManager.regDataSourceProp(conf.getPluginParam());
                sb.append(conf.getPluginName())
                        .append("/")
                        .append(conf.getPluginParam().getValue(IP, "IPUnknown"))
                        .append("/")
                        .append(conf.getPluginParam().getValue(DB, ""))
                        .append("/")
                        .append(conf.getPluginParam().getValue(TABLE, ""));
                if (i != writerConf.size() - 1) {
                    sb.append(",");
                }
            }
            //目标源 可能是多个
            target = sb.toString();

            //获取reader插件的配置类
            IParam readerPluginParam = pluginReg
                    .get(readerConf.getPluginName());

            log.info("Start Reader Threads!");
            ReaderManager readerManager = ReaderManager.getInstance(
                    storageManager, monitorManager);
            //多线程读取数据
            readerManager.run(readerConf, readerPluginParam);

            log.info("Start Writer Threads!");
            writerManager = WriterManager.getInstance(storageManager,
                    monitorManager, writerNum);
            writerManager.run(writerConf, pluginReg);

            int intervalCount = 0;
            int statusCheckInterval = engineConf.getIntValue(
                    EngineConfParamKey.STATUS_CHECK_INTERVAL,
                    STATUS_CHECK_INTERVAL);
            int monitorInfoDisplayPeriod = engineConf.getIntValue(
                    EngineConfParamKey.MONITOR_INFO_DISPLAY_PERIOD,
                    INFO_SHOW_PERIOD);
            writerConsistency = engineConf.getBooleanValue(
                    EngineConfParamKey.WRITER_CONSISTENCY, false);

            while (true) {
                intervalCount++;
                statusCode = checkStatus(readerManager, writerManager,
                        monitorManager, storageManager);
                status = JobStatus.fromStatus(statusCode);
                if (status == null) {
                    log.error("status = " + statusCode
                            + ".This should never happen");
                    return JobStatus.FAILED.getStatus();
                }
                // 读取失败 需要回滚 全部
                if (status == JobStatus.FAILED
                        || (status.getStatus() >= JobStatus.FAILED.getStatus() && status
                        .getStatus() < JobStatus.WRITE_FAILED
                        .getStatus())) {
                    log.error("Nebula wormhole Job is Failed!");
                    writerManager.rollbackAll();
                    break;
                } else if (status == JobStatus.PARTIAL_FAILED
                        || status.getStatus() >= JobStatus.WRITE_FAILED
                        .getStatus()) {
                    Set<String> failedIDs = getFailedWriterIDs(writerManager,
                            monitorManager);
                    log.error("Some of the writer is Failed:"
                            + failedIDs.toString());
                    writerManager.rollback(failedIDs);
                    break;
                } else if (status == JobStatus.SUCCESS_WITH_ERROR) {
                    log.error("DTS Job is Completed successfully with a few abnormal data");
                    break;
                } else if (status == JobStatus.SUCCESS) {
                    log.info("DTS Job is Completed successfully!");
                    break;
                }
                // Running
                else if (status == JobStatus.RUNNING) {
                    if (intervalCount % monitorInfoDisplayPeriod == 0) {
                        log.info(monitorManager.realtimeReport());
                    }
                    try {
                        Thread.sleep(statusCheckInterval);
                    } catch (InterruptedException e) {
                        log.error("Sleep of main thread is interrupted!",
                                e);
                    }
                }
                //while end
            }

            //catch 错误
        } catch (DTSException e) {
            if (!status.isFailed()) {
                statusCode = e.getStatusCode();
                status = JobStatus.fromStatus(e.getStatusCode());
                if (status == null) {
                    log.error("status = " + statusCode
                            + ".This should never happen");
                    return JobStatus.FAILED.getStatus();
                }
            }

            if (JobStatus.fromStatus(e.getStatusCode()).equals(
                    JobStatus.ROLL_BACK_FAILED)) {
                log.error("Roll back failed: " + e.getPluginID(), e);
            } else {
                log.error("Nebula wormhole Job is Failed!", e);
                try {
                    writerManager.killAll();
                    writerManager.rollbackAll();
                } catch (Exception e1) {
                    log.error("Roll back all failed ", e1);
                }
            }
        } catch (InterruptedException e) {
            status = JobStatus.FAILED;
            log.error(
                    "DTS Job is Failed  as it is interrupted when prepare to read or write",
                    e);
        } catch (ExecutionException e) {
            status = JobStatus.FAILED;
            log.error(
                    "DTS Job is Failed  as it is failed when prepare to read or write",
                    e);
        } catch (TimeoutException e) {
            status = JobStatus.FAILED;
            log.error(
                    "DTS Job is Failed  as it is timeout when prepare to read or write",
                    e);
        } catch (Exception e) {
            if (!status.isFailed()) {
                status = JobStatus.FAILED;
            }
            log.error("DTS Job is Failed!", e);
            log.error("Unknown Exception occurs, will roll back all");
            try {
                if (writerManager != null) {
                    writerManager.killAll();
                    writerManager.rollbackAll();
                }
            } catch (Exception e1) {
                log.error("Roll back all failed ", e1);
            }

        } finally {
            time = new Date().getTime() - now.getTime();
            if (monitorManager != null) {
                DTSJobInfo jobInfo = monitorManager.getJobInfo(source,
                        target, time / 1000, status.getStatus(), now);
                JobDBUtil.insertOneJobInfo(jobInfo);
            }
            log.info(monitorManager.finalReport());
        }
        if (statusCode != JobStatus.RUNNING.getStatus()) {
            return statusCode;
        } else {
            return status.getStatus();
        }
    }

    public static void main(String[] args) {
        JobConf jobConf = null;
        IParam engineConf = null;
        Map<String, IParam> pluginConfs = null;
        try {
            // 任务配置
            jobConf = ParseXMLUtil.loadJobConf("d:\\job.xml");
            // 通用配置 包括使用什么缓存等
            engineConf = ParseXMLUtil.loadEngineConfig();
            // reader writer 插件配置
            pluginConfs = ParseXMLUtil.loadPluginConf();
        } catch (Exception e) {
            log.error("Configure file error occurs: ", e);
            System.exit(JobStatus.CONF_FAILED.getStatus());
        }
        // 初始化 引擎配置
        Engine engine = new Engine(engineConf, pluginConfs);
        // 依据方法传入或者post传入 DTS只做工具类
        engine.startWork(jobConf);

    }

    private List<StorageConf> createStorageConfs(JobConf jobConf) {
        // 获取目标端信息
        List<JobPluginConf> writerConfList = jobConf.getWriterConfs();
        List<StorageConf> result = new ArrayList<StorageConf>();

        for (JobPluginConf jobPluginConf : writerConfList) {
            StorageConf storageConf = new StorageConf();
            storageConf.setId(jobPluginConf.getId());
            storageConf.setStorageClassName(engineConf.getValue(
                    // 如果文档里面没有 就采用默认的 内存缓存
                    EngineConfParamKey.STORAGE_CLASS_NAME,
                    DEFAULT_STORAGE_CLASS_NAME));
            // 该版本均是按行分割
            storageConf.setLineLimit(engineConf.getIntValue(
                    EngineConfParamKey.STORAGE_LINE_LIMIT,
                    DEFAULT_STORAGE_LINE_LIMIT));
            // 读取最大字节数
            storageConf.setByteLimit(engineConf.getIntValue(
                    EngineConfParamKey.STORAGE_BYTE_LIMIT,
                    DEFAULT_STORAGE_BYTE_LIMIT));
            storageConf.setDestructLimit(engineConf.getIntValue(
                    EngineConfParamKey.STORAGE_DESTRUCT_LIMIT,
                    DEFAULT_STORAGE_DESTRUCT_LIMIT));
            storageConf.setPeriod(engineConf.getIntValue(
                    EngineConfParamKey.MONITOR_INFO_DISPLAY_PERIOD,
                    INFO_SHOW_PERIOD));
            storageConf.setWaitTime(engineConf.getIntValue(
                    EngineConfParamKey.STORAGE_WAIT_TIME,
                    DEFAULT_STORAGE_WAIT_TIME));
            storageConf
                    .setPeripheralTimeout(engineConf
                            .getIntValue(EngineConfParamKey.READER_AND_WRITER_PERIPHERAL_TIMEOUT));
            result.add(storageConf);
        }
        return result;
    }

    /**
     * @param readerManager
     * @param writerManager
     * @param monitorManager
     * @param storageManager
     * @return
     */
    private int checkStatus(ReaderManager readerManager,
                            WriterManager writerManager, MonitorManager monitorManager,
                            StorageManager storageManager) {
        boolean readerTerminated = readerManager.terminate();
        if (readerTerminated) {
            storageManager.closeInput();
        }
        boolean writerTerminated = writerManager.terminate(writerConsistency);
        boolean readerSuccess = true;
        if (readerTerminated) {
            readerSuccess = readerManager.isSuccess();
        }
        Set<String> failedWriterIDs = new HashSet<String>();
        if (writerTerminated) {
            failedWriterIDs = writerManager.getFailedWriterID();
        }
        if (readerTerminated && writerTerminated && readerSuccess
                && failedWriterIDs.size() == 0 && monitorManager.isJobSuccess()) {
            return JobStatus.SUCCESS.getStatus();
        } else if (readerTerminated && writerTerminated && readerSuccess
                && failedWriterIDs.size() == 0
                && !monitorManager.isJobSuccess()) {
            Set<FailedInfo> failedInfoSet = monitorManager.getFailedInfo();
            for (FailedInfo fi : failedInfoSet) {
                String writerID = fi.getFailedWriterID();
                int size = fi.getFailedLines();
                if (size >= 0) {
                    log.info(writerID + ": " + fi.getFailedLines()
                            + " lines failed to write");
                } else {
                    log.info(writerID + ": " + (-fi.getFailedLines())
                            + " lines duplicated to write");
                }
                if (Math.abs(fi.getFailedLines()) >= writerManager
                        .getFailedLinesThreshold(writerID)) {
                    if (writerConsistency) {
                        return JobStatus.FAILED.getStatus();
                    } else {
                        return JobStatus.PARTIAL_FAILED.getStatus();
                    }
                }
            }
            return JobStatus.SUCCESS_WITH_ERROR.getStatus();
        } else if ((!readerTerminated && writerTerminated)) {
            int writerErrorCode = writerManager.getErrorCode();
            if (writerErrorCode != Integer.MAX_VALUE) {
                return writerErrorCode;
            }
            return JobStatus.FAILED.getStatus();
        } else if (readerTerminated && !readerSuccess) {
            return readerManager.getStatus();
        } else if (readerTerminated && readerSuccess && writerTerminated
                && failedWriterIDs.size() > 0) {
            int error = writerManager.getErrorCode();
            log.info(error);
            if (error != Integer.MAX_VALUE) {
                return error;
            }
            if (writerConsistency) {
                return JobStatus.FAILED.getStatus();
            } else {
                return JobStatus.PARTIAL_FAILED.getStatus();
            }
        }

        return JobStatus.RUNNING.getStatus();
    }

    /**
     * 获取失败的id
     *
     * @param writerManager
     * @param monitorManager
     * @return
     */
    private Set<String> getFailedWriterIDs(WriterManager writerManager,
                                           MonitorManager monitorManager) {
        Set<String> r = new HashSet<String>();
        Set<FailedInfo> failedInfoSet = monitorManager.getFailedInfo();
        for (FailedInfo fi : failedInfoSet) {
            String writerID = fi.getFailedWriterID();
            if (fi.getFailedLines() >= writerManager
                    .getFailedLinesThreshold(writerID)) {
                r.add(writerID);
            }
        }
        r.addAll(writerManager.getFailedWriterID());
        return r;
    }
}
