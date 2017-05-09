package com.jd.chen.dts.core.storage;

import com.jd.chen.dts.common.lord.IStorage;
import com.jd.chen.dts.common.utils.ReflectionUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class StorageManager {
    private static Log log = LogFactory.getLog(StorageManager.class);
    private Map<String, IStorage> storageMap = new HashMap<String, IStorage>();

    //获取实例类
    public StorageManager(List<StorageConf> confList) {
        for (StorageConf conf : confList) {
            if (conf == null || conf.getId() == null) {
                continue;
            }
            IStorage storage = ReflectionUtil.createInstanceByDefaultConstructor(conf.getStorageClassName(), IStorage.class);
            if (storage.init(conf.getId(), conf.getLineLimit(), conf.getByteLimit(), conf.getDestructLimit(), conf.getWaitTime())) {
                storage.getStat().setPeriodInSeconds(conf.getPeriod());
                storageMap.put(conf.getId(), storage);
            }
        }
    }

    /**
     * @return
     */
    public Map<String, IStorage> getStorageMap() {
        return storageMap;
    }

    /**
     * @return
     */
    public List<IStorage> getStorageForReader() {
        List<IStorage> result = new ArrayList<IStorage>();
        for (IStorage storage : storageMap.values()) {
            result.add(storage);
        }
        return result;
    }

    public IStorage getStorageForWriter(String id) {
        return storageMap.get(id);
    }

    public void closeInput() {
        for (String key : storageMap.keySet()) {
            storageMap.get(key).close();
        }
    }
}
