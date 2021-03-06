package com.jd.chen.dts.plugins.writer.mysqlWriter;

import com.jd.chen.dts.common.config.JobStatus;
import com.jd.chen.dts.common.exception.DTSException;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.ISourceCounter;
import com.jd.chen.dts.common.lord.ITargetCounter;
import com.jd.chen.dts.common.lord.IWriterPeriphery;
import com.jd.chen.dts.common.utils.DBSource;
import com.jd.chen.dts.common.utils.DBUtils;
import com.jd.chen.dts.core.thread.impl.AbstractPlugin;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * com.jd.chen.dts.plugins.writer.mysqlWriter.MysqlWriterPeriphery
 * Created by chenxiaolei3 on 2017/4/20.
 */
public class MysqlWriterPeriphery implements IWriterPeriphery {
    private static Log logger = LogFactory.getLog(MysqlWriterPeriphery.class);

    private Connection conn;

    private String username;

    private String password;

    private String ip;

    private String port = "3306";

    private String dbname;

    private String mysqlParams;

    private String encode;

    private String rollback;

    private String pre;

    private String post;

    private int concurrency = 1;

    private String countSql;

    private String writerID;

    private int errorCodeAdd;

    @Override
    public void prepare(IParam param, ISourceCounter counter) {
        register(param);
        if (pre.isEmpty()) {
            return;
        }
        try {
            conn = DBSource.getConnection(MysqlWriter.class, ip, writerID, dbname);
            String[] sqlArray = pre.split(";");
            for (String sql : sqlArray) {
                sql = sql.trim();
                if (sql.isEmpty()) {
                    continue;
                }
                DBUtils.update(conn, sql);
            }
            conn.close();
        } catch (Exception e) {
            logger.error(writerID + ": Mysql writer prepare failed. ");
            throw new DTSException(e, JobStatus.PRE_WRITE_FAILED.getStatus() + errorCodeAdd, writerID);
        }
    }

    @Override
    public void rollback(IParam param) {
        if (rollback.isEmpty()) {
            logger.error("Fail to roll back because rollback sql is empty!");
            return;
        }
        try {
            conn = DBSource.getConnection(MysqlWriter.class, ip, writerID, dbname);
            String[] sqlArray = rollback.split(";");
            for (String sql : sqlArray) {
                sql = sql.trim();
                if (sql.isEmpty()) {
                    continue;
                }
                DBUtils.update(conn, sql);
            }
            conn.close();
        } catch (Exception e) {
            logger.error(writerID + ": Mysql writer roll back failed. ");
            throw new DTSException(e, JobStatus.ROLL_BACK_FAILED.getStatus() + errorCodeAdd, writerID);
        }
    }

    @Override
    public void doPost(IParam param, ITargetCounter counter) {
        if (!countSql.isEmpty()) {
            try {
                conn = DBSource.getConnection(MysqlWriter.class, ip, writerID, dbname);
                ResultSet rs = DBUtils.query(conn, countSql);
                rs.next();
                int lines = rs.getInt(1);
                counter.setTargetLines(param.getValue("pluginName", ""), lines);
                rs.close();
                conn.close();
            } catch (Exception e) {
                logger.error("Mysql writer count line number failed. " + e.getMessage());
            }
        }
        if (!post.isEmpty()) {
            try {
                conn = DBSource.getConnection(MysqlWriter.class, ip, writerID, dbname);
                String[] sqlArray = post.split(";");
                for (String sql : sqlArray) {
                    sql = sql.trim();
                    if (sql.isEmpty()) {
                        continue;
                    }
                    DBUtils.update(conn, sql);
                }
                conn.close();
            } catch (Exception e) {
                logger.error(writerID + ": Mysql writer dopost failed. ");
                throw new DTSException(e, JobStatus.POST_WRITE_FAILED.getStatus() + errorCodeAdd, writerID);
            }
        }
    }

    private void init(IParam param) {
        /* for database connection */
        this.username = param.getValue(ParamKey.USER_NAME, "");
        this.password = param.getValue(ParamKey.PASSWORD, "");
        this.ip = param.getValue(ParamKey.IP, "");
        this.port = param.getValue(ParamKey.PORT, this.port);
        this.dbname = param.getValue(ParamKey.DBNAME, "");
        this.encode = param.getValue(ParamKey.ENCODING, "");
        this.mysqlParams = param.getValue(ParamKey.MYSQL_PRAMAS, "");
        this.rollback = param.getValue(ParamKey.ROLL_BACK, "");
        this.pre = param.getValue(ParamKey.PRE, "");
        this.post = param.getValue(ParamKey.POST, "");
        this.countSql = param.getValue(ParamKey.COUNT_SQL, "");
        this.writerID = param.getValue(AbstractPlugin.PLUGINID, "");
        int priority = param.getIntValue(ParamKey.PRIORITY, 0);
        this.errorCodeAdd = MysqlWriter.PLUGIN_NO * JobStatus.PLUGIN_BASE + priority * JobStatus.WRITER_BASE;
    }

    private void register(IParam param) {
        init(param);
        /* for connection session */
        Properties p = createProperties();
        try {
            DBSource.register(MysqlWriter.class, this.ip, this.writerID, this.dbname, p);
        } catch (Exception e) {
            throw new DTSException(e, JobStatus.WRITE_CONNECTION_FAILED.getStatus() + errorCodeAdd);
        }

    }

    private Properties createProperties() {
        Properties p = new Properties();

        String encodeDetail = "";

        if (!StringUtils.isBlank(this.encode)) {
            encodeDetail = "useUnicode=true&characterEncoding=" + this.encode + "&";
        }
        String url = "jdbc:mysql://" + this.ip + ":" + this.port + "/"
                + this.dbname + "?" + encodeDetail
                + "yearIsDateType=false&zeroDateTimeBehavior=convertToNull"
                + "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);
        if (!StringUtils.isBlank(this.mysqlParams)) {
            url = url + "&" + this.mysqlParams;
        }

        p.setProperty("driverClassName", "com.mysql.jdbc.Driver");
        p.setProperty("url", url);
        p.setProperty("username", username);
        p.setProperty("password", password);
        p.setProperty("maxActive", String.valueOf(concurrency + 2));
        //don't set initialSize, otherwise accessToUnderlyingConnectionAllowed will not be set successfully
        //p.setProperty("initialSize", String.valueOf(concurrency + 2));
        p.setProperty("maxIdle", "1");
        p.setProperty("maxWait", "1000");
        p.setProperty("defaultReadOnly", "false");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("validationQuery", "select 1 from dual");

        logger.debug(String.format("Mysql try connection: %s .", url));
        return p;
    }
}
