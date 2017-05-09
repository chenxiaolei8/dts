package com.jd.chen.dts.common.utils;

import com.jd.chen.dts.core.thread.IPlugin;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by chenxiaolei3 on 2017/4/20.
 */
public class DBSource {
    private static Log log = LogFactory.getLog(DBSource.class);
    public static final Map<String, DataSource> sourceInfoMap = new HashMap<String, DataSource>();

    private DBSource() {
    }

    public static boolean register(Class<? extends Object> clazz, String ip,
                                   String port, String dbname, Properties p) throws Exception {
        String id = genKey(clazz, ip, port, dbname);
        return register(id, p);
    }

    public static synchronized boolean register(String key, Properties p) throws Exception {
        boolean succeed = false;

        if (!sourceInfoMap.containsKey(key)) {
            BasicDataSource dataSource = null;
            try {
                dataSource = (BasicDataSource) BasicDataSourceFactory
                        .createDataSource(p);
            } catch (Exception e) {
                log.error(String.format(
                        "Key [%s] register database pool failed .", key));
                throw e;
            }
            if (null != dataSource) {
                dataSource.setAccessToUnderlyingConnectionAllowed(true);
                sourceInfoMap.put(key, dataSource);
                log.info(String.format(
                        "Key [%s] register database pool successfully .", key));
                succeed = true;
            } else {
                log.error(String.format(
                        "Key [%s] register database pool failed .", key));
                throw new Exception("register database pool failed .");
            }
        } else {
            log.error(String.format("Key [%s] already in database pool .",
                    key));
        }
        return succeed;
    }

    public static DataSource getDataSource(Class<? extends IPlugin> clazz,
                                           String ip, String port, String dbname) throws Exception {
        return getDataSource(genKey(clazz, ip, port, dbname));
    }

    public static synchronized DataSource getDataSource(String key) throws Exception {
        DataSource source = sourceInfoMap.get(key);
        if (null == source) {
            throw new Exception(String.format(
                    "Cannot get DataSource specified by key [%s] .", key));
        }
        return source;
    }

    public static String genKey(Class<? extends Object> clazz, String ip,
                                String port, String dbname) {
        String str = clazz.getCanonicalName() + "_" + ip + "_" + port + "_"
                + dbname;
        return md5(str);
    }

    private static String md5(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes());
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer(32);
            for (byte aB : b) {
                i = aB;
                if (i < 0) {
                    i += 256;
                }
                if (i < 16) {
                    buf.append("0");
                }
                buf.append(Integer.toHexString(i));
            }
            return buf.toString().substring(8, 24);
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage());
            return key;
        }
    }

    public static Connection getConnection(Class<? extends Object> clazz,
                                           String ip, String port, String dbname) throws Exception {
        return getConnection(genKey(clazz, ip, port, dbname), port);
    }

    public static synchronized Connection getConnection(String id, String pluginID) throws Exception {
        Connection c = null;
        BasicDataSource dataSource = (BasicDataSource) sourceInfoMap.get(id);
        try {
            c = dataSource.getConnection();
        } catch (SQLException e) {
            log.error(e);
            throw e;
        }
        if (null != c) {
            log.debug(String.format(
                    pluginID + "-Key [%s] connect to database pool successfully .", id));
        } else {
            log.error(String.format(
                    pluginID + "-Key [%s]  connect to database pool failed .", id));
            throw new Exception("Connection key [%s] error .");
        }
        return c;
    }
}
