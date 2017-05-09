package com.jd.chen.dts.common.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class StringUtil {
    private static Log log = LogFactory.getLog(StringUtil.class);
    private static final String VARIABLE_PATTERN = "(\\$)\\{(\\w+)\\}";

    private StringUtil() {

    }

    public static String replaceEnvironmentVariables(String text) {
        Pattern pattern = Pattern.compile(VARIABLE_PATTERN);
        Matcher matcher = pattern.matcher(text);

        while (matcher.find()) {
            log.info("replace " + matcher.group(2) +
                    " with " + System.getenv(matcher.group(2)));

            text = StringUtils.replace(text, matcher.group(),
                    StringUtils.defaultString(System.getenv(matcher.group(2)), matcher.group()));
        }
        return text;
    }
}
