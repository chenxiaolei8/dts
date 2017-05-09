package com.jd.chen.dts.common.config;

import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class JobConf {

    private String id;

    private JobPluginConf readerConf;

    private List<JobPluginConf> writerConfs;

    /**
     * 获得Reader的配置
     *
     * @return
     */
    public JobPluginConf getReaderConf() {
        return readerConf;
    }

    /**
     * 设置Reader插件的配置
     *
     * @param readerConf
     */
    public void setReaderConf(JobPluginConf readerConf) {
        this.readerConf = readerConf;
    }

    /**
     * 获取Writer插件的配置。返回值是列表，
     * 这是为了适应多个数据目的地。
     *
     * @return
     */
    public List<JobPluginConf> getWriterConfs() {
        return this.writerConfs;
    }

    /**
     * 设置Writer插件的配置
     *
     * @param writerConfs
     */
    public void setWriterConfs(List<JobPluginConf> writerConfs) {
        this.writerConfs = writerConfs;
    }

    /**
     * 获取写入端个数 可能一个数据源对应多个段 本版本设置一对多
     *
     * @return
     */
    public int getWriterNum() {
        return this.writerConfs.size();
    }

    /**
     * 设置本次服务的id
     *
     * @param id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * 获取本次jobid
     *
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * 显示配置信息
     *
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(300);
        sb.append(String.format("\njob:%s", this.getId()));
        sb.append("\nReader conf:");
        sb.append(this.readerConf.toString());
        sb.append(String.format("\n\nWriter conf [num %d]:", this.writerConfs.size()));
        for (JobPluginConf dpc : this.writerConfs) {
            sb.append(dpc.toString());
        }
        return sb.toString();
    }

}
