/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.server.service;

import com.taobao.diamond.server.utils.SystemConfig;
import com.taobao.diamond.utils.ResourceUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;


/**
 * @author boyan
 * @date 2010-5-6
 */
@Service
public class NotifyService {
    private static final int TIMEOUT = 5000;

    private final String URL_PREFIX = "/diamond-server/notify.do";

    private final String PROTOCOL = "http://";

    private final Properties nodeProperties = new Properties();

    static final Log log = LogFactory.getLog(NotifyService.class);


    Properties getNodeProperties() {
        return this.nodeProperties;
    }


    @PostConstruct
    public void loadNodes() {
        InputStream in = null;
        try {
            in = ResourceUtils.getResourceAsStream("node.properties");
            nodeProperties.load(in);
        } catch (IOException e) {
            log.error("load node.properties fail");
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) {
                log.error("close node.properties fail", e);
            }
        }
        log.info("node list:" + nodeProperties);
    }

    public void notifyConfigInfoChange(String dataId, String group) {
        Enumeration<?> enu = nodeProperties.propertyNames();
        while (enu.hasMoreElements()) {
            String address = (String) enu.nextElement();
            if (address.contains(SystemConfig.LOCAL_IP)) {
                continue;
            }
            String urlString = generateNotifyConfigInfoPath(dataId, group, address);
            final String result = invokeURL(urlString);
            log.info("notify node address=" + address + "group changed info" + result);
        }
    }


    String generateNotifyConfigInfoPath(String dataId, String group, String address) {
        String specialUrl = this.nodeProperties.getProperty(address);
        String urlString = PROTOCOL + address + URL_PREFIX;
        if (specialUrl != null && StringUtils.hasLength(specialUrl.trim())) {
            urlString = specialUrl;
        }
        urlString += "?method=notifyConfigInfo&dataId=" + dataId + "&group=" + group;
        return urlString;
    }


    /**
     * http get
     *
     * @param urlString
     * @return
     */
    private String invokeURL(String urlString) {
        HttpURLConnection conn = null;
        URL url = null;
        try {
            url = new URL(urlString);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(TIMEOUT);
            conn.setReadTimeout(TIMEOUT);
            conn.setRequestMethod("GET");
            conn.connect();
            InputStream urlStream = conn.getInputStream();
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(urlStream));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
            } finally {
                if (reader != null)
                    reader.close();
            }
            return sb.toString();

        } catch (Exception e) {
            log.error("http invoke fail,url=" + urlString, e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return "error";
    }
}
