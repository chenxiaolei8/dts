package com.jd.chen.dts.plugins.reader.mysqlReader;

import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.exception.DTSException;
import com.jd.chen.dts.common.utils.DBResultSetSender;
import com.jd.chen.dts.common.utils.DBSource;
import com.jd.chen.dts.common.utils.DBUtils;
import com.jd.chen.dts.core.thread.ILineSender;
import com.jd.chen.dts.core.thread.IReader;
import com.jd.chen.dts.core.thread.impl.AbstractPlugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * com.jd.chen.dts.plugins.reader.mysqlReader.MysqlReader
 * Created by chenxiaolei3 on 2017/4/20.
 */
public class MysqlReader extends AbstractPlugin implements IReader {
    private static Log log = LogFactory.getLog(MysqlReader.class);
    static final int PLUGIN_NO = 1;
    //允许失败1000行
    static final int ERROR_CODE_ADD = JobStatus.PLUGIN_BASE * PLUGIN_NO;

    private Connection conn;

    private String ip = "";

    private String port = "3306";

    private String dbname;

    private String sql;


    public void read(ILineSender lineSender) {
        DBResultSetSender proxy = DBResultSetSender.newSender(lineSender);
        proxy.setMonitor(getMonitor());
        proxy.setDateFormatMap(genDateFormatMap());
        if (sql.isEmpty()) {
            log.error("Sql for mysqlReader is empty.");
            throw new DTSException("Sql for mysqlReader is empty.", JobStatus.READ_FAILED.getStatus() + ERROR_CODE_ADD);
        }
        log.debug(String.format("MysqlReader start to query %s .", sql));
        for (String sqlItem : sql.split(";")) {
            sqlItem = sqlItem.trim();
            if (sqlItem.isEmpty()) {
                continue;
            }
            log.debug(sqlItem);
            ResultSet rs = null;
            try {
                rs = DBUtils.query(conn, sqlItem);
                proxy.sendToWriter(rs);
                proxy.flush();
            } catch (SQLException e) {
                log.error("Mysql read failed", e);
                throw new DTSException(e, JobStatus.READ_FAILED.getStatus() + ERROR_CODE_ADD);
            } catch (DTSException e1) {
                e1.setStatusCode(e1.getStatusCode() + ERROR_CODE_ADD);
                throw e1;
            } finally {
                if (null != rs) {
                    try {
                        DBUtils.closeResultSet(rs);
                    } catch (SQLException e) {
                        log.error("MysqlReader close resultset error ");
                        throw new DTSException(e, JobStatus.READ_FAILED.getStatus() + ERROR_CODE_ADD);
                    }
                }
            }
        }
    }

    public void init() {
        /* for database connection */
        this.ip = getParam().getValue(ParamKey.ip, "");
        this.port = getParam().getValue(ParamKey.port, this.port);
        this.dbname = getParam().getValue(ParamKey.dbname, "");
        this.sql = getParam().getValue(ParamKey.sql, "").trim();
    }

    public void connection() {
        try {
            conn = DBSource.getConnection(this.getClass(), ip, port, dbname);
        } catch (Exception e) {
            throw new DTSException(e, JobStatus.READ_CONNECTION_FAILED.getStatus() + ERROR_CODE_ADD);
        }
    }

    public void finish() {
        try {
            if (conn != null) {
                conn.close();
            }
            conn = null;
        } catch (SQLException e) {
            log.error("Close connection failed", e);
        }
    }

    private Map<String, SimpleDateFormat> genDateFormatMap() {
        Map<String, SimpleDateFormat> mapDateFormat = new HashMap<String, SimpleDateFormat>();
        mapDateFormat.clear();
        mapDateFormat.put("datetime", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss"));
        mapDateFormat.put("timestamp", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss"));
        mapDateFormat.put("time", new SimpleDateFormat("HH:mm:ss"));
        mapDateFormat.put("date", new SimpleDateFormat(
                "yyyy-MM-dd"));
        return mapDateFormat;
    }
}
