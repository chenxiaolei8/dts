package com.jd.chen.dts.common.utils;

import com.jd.chen.dts.common.lord.IPluginMonitor;
import com.jd.chen.dts.core.thread.ILine;
import com.jd.chen.dts.core.thread.ILineSender;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chenxiaolei3 on 2017/4/20.
 */
public class DBResultSetSender {
    private static Log log = LogFactory.getLog(DBResultSetSender.class);
    private ILineSender sender;

    private int columnCount;

    private IPluginMonitor monitor;

    private Map<String, SimpleDateFormat> dateFormatMap = new HashMap<String, SimpleDateFormat>();

    private SimpleDateFormat[] timeMap = null;

    public static DBResultSetSender newSender(ILineSender sender) {
        return new DBResultSetSender(sender);
    }

    public DBResultSetSender(ILineSender lineSender) {
        this.sender = lineSender;
    }

    public void setMonitor(IPluginMonitor iMonitor) {
        this.monitor = iMonitor;
    }

    public void flush() {
        if (sender != null) {
            sender.flush();
        }
    }

    public void setDateFormatMap(Map<String, SimpleDateFormat> dateFormatMap) {
        this.dateFormatMap = dateFormatMap;
    }

    private void setColumnTypes(ResultSet resultSet) throws SQLException {
        timeMap = new SimpleDateFormat[columnCount + 1];

        ResultSetMetaData rsmd = resultSet.getMetaData();

        for (int i = 1; i <= columnCount; i++) {
            String type = rsmd.getColumnTypeName(i).toLowerCase().trim();
            if (this.dateFormatMap.containsKey(type)) {
                timeMap[i] = this.dateFormatMap.get(type);
            }
        }
    }

    private void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    /**
     * 处理数据
     * @param resultSet
     * @throws SQLException
     */
    public void sendToWriter(ResultSet resultSet) throws SQLException {
        String item = null;
        Timestamp ts = null;
        setColumnCount(resultSet.getMetaData().getColumnCount());
        //设置特殊的时间类型
        setColumnTypes(resultSet);
        while (resultSet.next()) {
            ILine line = sender.createNewLine();
            try {
                /* TODO: date format need to handle by transfomer plugin */
                for (int i = 1; i <= columnCount; i++) {
                    if (null != timeMap[i]) {
                        ts = resultSet.getTimestamp(i);
                        if (null != ts) {
                            item = timeMap[i].format(ts);
                        } else {
                            item = null;
                        }
                    } else {
                        item = resultSet.getString(i);
                    }
                    line.addField(item);
                }
                Boolean b = sender.send(line);
                if (null != monitor) {
                    if (b) {
                        monitor.increaseSuccessLines();
                    } else {
                        monitor.increaseFailedLines();
                    }
                }
            } catch (SQLException e) {
                monitor.increaseFailedLines();
                log.error(e.getMessage() + "| One dirty line : " + line.toString('\t'));
            }
        }

    }
}
