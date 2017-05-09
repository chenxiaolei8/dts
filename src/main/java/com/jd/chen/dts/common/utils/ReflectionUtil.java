package com.jd.chen.dts.common.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class ReflectionUtil {
    private static Log log = LogFactory.getLog(ReflectionUtil.class);

    /**
     * 一般来说 如果新加入插件 可以通过指定jar包然后 直接添加 反射实现
     *
     * @param className
     * @param type
     * @param jarLoader
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T createInstanceByDefaultConstructor(String className, Class<T> type,
                                                           JarLoader jarLoader) {
        try {
            Class<T> clazz = null;
            if (jarLoader != null) {
                clazz = (Class<T>) jarLoader.loadClass(className);
            }
            if (clazz == null) {
                clazz = (Class<T>) Class.forName(className);
            }
            return clazz.newInstance();
        } catch (Exception e) {
            log.error("Exception occurs when creating " + className, e);
            return null;
        }
    }

    public static <T> T createInstanceByDefaultConstructor(String className, Class<T> type) {
        return createInstanceByDefaultConstructor(className, type, null);
    }
}
