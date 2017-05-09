package com.jd.chen.dts.common.utils;

import java.io.File;

import com.jd.chen.dts.common.config.JobConf;
import com.jd.chen.dts.common.config.JobPluginConf;
import com.jd.chen.dts.common.lord.IParam;
import com.jd.chen.dts.common.lord.impl.DefaultParam;
import com.jd.chen.dts.core.Engine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.apache.commons.io.FileUtils;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class ParseXMLUtil {
    private static Log log = LogFactory.getLog(ParseXMLUtil.class);

    private ParseXMLUtil() {
    }

    /**
     * 依据传入的配置路径 配置reader writer job 配置参数
     *
     * @param filename
     * @return
     */
    public static JobConf loadJobConf(String filename) {
        //jobId reader配置 writer配置
        JobConf job = new JobConf();
        Document document = null;
        try {
            String xml = FileUtils
                    .readFileToString(new File(filename), "UTF-8");
            xml = StringUtil.replaceEnvironmentVariables(xml);
            document = DocumentHelper.parseText(xml);
        } catch (IOException e) {
            log.error(String.format("DTS can't find job conf file: %s.",
                    filename));
        } catch (DocumentException e) {
            log.error(String.format("Parse %s to document failed.", filename));
        }
        String xpath = "/job";
        Element jobE = (Element) document.selectSingleNode(xpath);
        // 获取jobId 未发现 默认DTS_id_not_found
        String jobId = jobE.attributeValue("id", "DTS_id_not_found")
                .trim();
        job.setId(jobId);
        JobPluginConf readerJobConf = new JobPluginConf();
        Element readerE = (Element) jobE.selectSingleNode(xpath + "/reader");
        Element readerPluginE = (Element) readerE.selectSingleNode("plugin");
        // readerId
        String readerId = readerE.attributeValue("id");
        String readerName = readerPluginE.getStringValue().trim().toLowerCase();
        readerJobConf.setPluginName(readerName);
        readerJobConf.setId(readerId == null ? "reader-id-" + readerName
                : readerId.trim());
        //获取reader参数
        Map<String, String> readerPluginParamMap = getParamMap(readerE);
        IParam readerPluginParam = new DefaultParam(readerPluginParamMap);
        readerJobConf.setPluginParam(readerPluginParam);


        List<JobPluginConf> writerJobConfs = new ArrayList<JobPluginConf>();
        List<Element> writerEs = (List<Element>) document.selectNodes(xpath
                + "/writer");
        for (Element writerE : writerEs) {
            JobPluginConf writerPluginConf = new JobPluginConf();

            Element writerPluginE = (Element) writerE
                    .selectSingleNode("plugin");
            String writerName = writerPluginE.getStringValue().trim()
                    .toLowerCase();
            String writerId = writerE.attributeValue("id");
            writerPluginConf.setPluginName(writerName);
            writerPluginConf.setId(writerId == null ? "writer-id-"
                    + writerEs.indexOf(writerE) + "-" + writerName : writerId
                    .trim());

            Map<String, String> writerPluginParamMap = getParamMap(writerE);

            IParam writerPluginParam = new DefaultParam(writerPluginParamMap);

            writerPluginConf.setPluginParam(writerPluginParam);
            writerJobConfs.add(writerPluginConf);
        }

        job.setReaderConf(readerJobConf);
        job.setWriterConfs(writerJobConfs);

        return job;
    }

    public static IParam loadEngineConfig() {
        File file = new File(Environment.ENGINE_CONF);
        Document doc = null;
        try {
            String xml = FileUtils.readFileToString(file);
            doc = DocumentHelper.parseText(xml);
        } catch (IOException e) {
            log.error("Can not find engine.xml .");
        } catch (DocumentException e) {
            log.error("Can not parse engine.xml .");
        }
        String xpath = "/engine";
        Element engineE = (Element) doc.selectSingleNode(xpath);
        Map<String, String> engineParam = getParamMap(engineE);
//        测试生成的map
//        for (Map.Entry<String,String> fd:engineParam.entrySet()){
//            System.out.println(fd.getKey()+" ===== "+fd.getValue());
//        }
        return new DefaultParam(engineParam);
    }

    /**
     * 读取R and W 插件的信息
     *
     * @return
     */
    public static Map<String, IParam> loadPluginConf() {
        File f = new File(Environment.PLUGINS_CONF);
        Document doc = null;

        try {
            String xml = FileUtils.readFileToString(f);
            doc = DocumentHelper.parseText(xml);
        } catch (IOException e) {
            log.error(e.getMessage());
        } catch (DocumentException e) {
            log.error("the document could not be parsed. " + e.getMessage());
        }

        Map<String, IParam> pluginsMap = new HashMap<String, IParam>();
        String xpath = "/plugins/plugin";
        List<Element> pluginsEs = (List<Element>) doc.selectNodes(xpath);
        for (Element pluginsE : pluginsEs) {
            Map<String, String> pluginParamsMap = getParamMap(pluginsE);

            if (pluginParamsMap.containsKey("name")) {
                IParam plugin = new DefaultParam(pluginParamsMap);
                pluginsMap.put(pluginParamsMap.get("name"), plugin);
            } else {
                log.error(String
                        .format("plugin configure file can't find xpath \"%s\" plugin name",
                                pluginsE.getPath()));
            }
//            for (Map.Entry<String, String> fd : pluginParamsMap.entrySet()) {
//                System.out.println(fd.getKey() + " ===== " + fd.getValue());
//            }
        }
//        测试生成的map
//        for (Map.Entry<String, IParam> fd : pluginsMap.entrySet()) {
//            System.out.println(fd.getKey() + " ===== " + fd.getValue());
//        }
        return pluginsMap;
    }

    public static void main(String[] args) {
        Map<String, IParam> stringIParamMap = loadPluginConf();
    }

    /**
     * 遍历job 插件的配置元素
     * @param rootElement
     * @return
     */
    private static Map<String, String> getParamMap(Element rootElement) {
        Map<String, String> paramMap = new HashMap<String, String>();
        List<Element> paramsEs = (List<Element>) rootElement.selectNodes("./*");
        for (Element paramsE : paramsEs) {
            paramMap.put(paramsE.getName().trim(), paramsE.getStringValue());
        }
        return paramMap;
    }

}
