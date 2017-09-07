/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.util.Enumeration;
import java.util.Properties;

public class LoggerInit {
    static public final String LOG_NAME_CONFIG_DATA = "DiamondConfigDataLog";

    static public final Log log = LogFactory.getLog(LoggerInit.class);

    static private volatile boolean initOK = false;

    static private Properties defaultProperties = new Properties();

    static {
        defaultProperties.put("log4j.logger.DiamondConfigDataLog", "info, DiamondConfigDataLogFile");
        defaultProperties.put("log4j.additivity.DiamondConfigDataLog", "false");
        defaultProperties.put("log4j.appender.DiamondConfigDataLogFile", "org.apache.log4j.DailyRollingFileAppender");
        defaultProperties.put("log4j.appender.DiamondConfigDataLogFile.DatePattern", "'.'yyyy-MM-dd");
        defaultProperties.put("log4j.appender.DiamondConfigDataLogFile.File", "diamond_config_data.log");
        defaultProperties.put("log4j.appender.DiamondConfigDataLogFile.layout", "org.apache.log4j.PatternLayout");
        defaultProperties.put("log4j.appender.DiamondConfigDataLogFile.layout.ConversionPattern",
                "%d{MM-dd HH:mm:ss} - %m%n");
        defaultProperties.put("log4j.appender.DiamondConfigDataLogFile.Append", "true");
    }


    public static void initLogFromBizLog() {

        if (initOK) {
            return;
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(LoggerInit.class.getClassLoader());

        try {
            PropertyConfigurator.configure(defaultProperties);

            FileAppender bizFileAppender = getFileAppender(Logger.getRootLogger());
            if (null == bizFileAppender) {
                log.warn("Create new ROOT LOGGER FileAppender!!!");
                bizFileAppender = new FileAppender();
                bizFileAppender.setFile(System.getProperty("user.home") + "/diamond/logs/diamond_config_data.log");
            }

            setFileAppender(bizFileAppender, LOG_NAME_CONFIG_DATA);

            initOK = true;
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }


    private static void setFileAppender(FileAppender bizFileAppender, String logName) {
        FileAppender fileAppender = getFileAppender(Logger.getLogger(logName));
        String bizLogDir = new File(bizFileAppender.getFile()).getParent();

        File newLogFile = new File(bizLogDir, fileAppender.getFile());

        fileAppender.setFile(newLogFile.getAbsolutePath());
        fileAppender.activateOptions();
        log.warn("Log name=" + logName + "Appender path:" + newLogFile.getAbsolutePath());
    }


    static private FileAppender getFileAppender(Logger logger) {
        FileAppender fileAppender = null;
        for (Enumeration<?> appenders = logger.getAllAppenders(); (null == fileAppender) && appenders.hasMoreElements(); ) {
            Appender appender = (Appender) appenders.nextElement();
            if (FileAppender.class.isInstance(appender)) {
                fileAppender = (FileAppender) appender;
            }
        }
        return fileAppender;
    }
}
