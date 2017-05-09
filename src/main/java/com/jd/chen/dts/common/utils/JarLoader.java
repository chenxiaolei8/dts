package com.jd.chen.dts.common.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public final class JarLoader extends URLClassLoader {
    private static Log log = LogFactory.getLog(JarLoader.class);

    public static JarLoader getInstance(String[] paths) {
        try {
            return new JarLoader(paths);
        } catch (Exception e) {
            return null;
        }
    }

    public static JarLoader getInstance(String path) {
        return getInstance(new String[]{path});
    }

    private JarLoader(String[] paths) {
        this(paths, JarLoader.class.getClassLoader());
    }

    private JarLoader(String[] paths, ClassLoader parent) {
        super(getUrls(paths), parent);
    }
































    private static URL[] getUrls(String[] paths) {
        if (null == paths || 0 == paths.length) {
            throw new IllegalArgumentException("Paths cannot be empty .");
        }

        List<URL> urls = new ArrayList<URL>();
        for (String path : paths) {
            urls.addAll(Arrays.asList(getUrl(path)));
        }

        return urls.toArray(new URL[0]);
    }


    private static URL[] getUrl(String path) {
        /* check path exist */
        if (null == path || StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Path cannot be empty .");
        }

        File jarPath = new File(path);
        if (!jarPath.exists() || !jarPath.isDirectory()) {
            throw new IllegalArgumentException("Path must be directory .");
        }

		/* set filter */
        FileFilter jarFilter = new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".jar");
            }
        };

		/* iterate all jar */
        File[] allJars = new File(path).listFiles(jarFilter);
        URL[] jarUrls = new URL[allJars.length];

        for (int i = 0; i < allJars.length; i++) {
            try {
                jarUrls[i] = allJars[i].toURI().toURL();
            } catch (MalformedURLException e) {
                log.error("Error in getting jar URL!", e);
                throw new RuntimeException("Error in getting jar URL", e);
            }
            log.debug(jarUrls[i]);
        }

        return jarUrls;
    }


}
