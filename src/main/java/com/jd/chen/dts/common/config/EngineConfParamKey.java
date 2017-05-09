package com.jd.chen.dts.common.config;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public interface EngineConfParamKey {
    String STORAGE_CLASS_NAME = "storageClassName";

    String STORAGE_LINE_LIMIT = "storageLineLimit";

    String STORAGE_BYTE_LIMIT = "storageByteLimit";

    String STORAGE_DESTRUCT_LIMIT = "storageDistructLimit";

    String STORAGE_WAIT_TIME = "storageWaitTime";

    String STATUS_CHECK_INTERVAL = "statusCheckInterval";

    String MONITOR_INFO_DISPLAY_PERIOD = "monitorInfoDisplayPeriod";

    String WRITER_CONSISTENCY = "writerConsistency";

    String READER_AND_WRITER_PERIPHERAL_TIMEOUT = "readerAndWriterPeripheralTimeout";
}
