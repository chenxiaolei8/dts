package com.jd.chen.dts.plugins.writer.hdfsWriter;

import com.hadoop.compression.lzo.LzoIndexer;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.ISourceCounter;
import com.jd.chen.dts.common.lord.ITargetCounter;
import com.jd.chen.dts.common.lord.IWriterPeriphery;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

/**
 * Created by chenxiaolei3 on 2017/4/24.
 */
public class HdfsWriterPeriphery implements IWriterPeriphery {
    private static Log log = LogFactory.getLog(HdfsWriterPeriphery.class);
    private static final int HIVE_TABLE_ADD_PARTITION_PARAM_NUMBER = 2;
    private static final String HIDDEN_FILE_PREFIX = "_";
    private static final int MAX_LZO_CREATION_TRY_TIMES = 3;
    private static final long LZO_CREATION_TRY_INTERVAL_IN_MILLIS = 10000L;

    private String dir = "";
    private String prefixname = "";
    private int concurrency = 1;
    private String codecClass = "";
    private String fileType = "TXT";
    private String suffix = "";
    private boolean lzoCompressed = false;
    private String hiveTableAddPartitionOrNot = "false";
    private String hiveTableAddPartitionCondition = "";

    private final String ADD_PARTITION_SQL = "alter table {0} add if not exists partition({1}) location ''{2}'';";

    private FileSystem fs;
    private LzoIndexer lzoIndexer;

    @Override
    public void rollback(IParam param) {

    }

    @Override
    public void prepare(IParam param, ISourceCounter counter) {

    }

    @Override
    public void doPost(IParam param, ITargetCounter counter) {

    }
}
