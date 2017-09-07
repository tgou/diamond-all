/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.server.utils;

import com.taobao.diamond.utils.ResourceUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Properties;


public class SystemConfig {

    private static final Log log = LogFactory.getLog(SystemConfig.class);

    private static int dumpConfigInterval = 600;

    public static final String LOCAL_IP = getHostAddress();

    static {
        InputStream in = null;
        try {
            in = ResourceUtils.getResourceAsStream("system.properties");
            Properties props = new Properties();
            props.load(in);
            dumpConfigInterval = Integer.parseInt(props.getProperty("dump_config_interval", "600"));
        } catch (IOException e) {
            log.error("load system.properties error", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error("close system.properties error", e);
                }
            }
        }
    }


    public SystemConfig() {

    }


    public static int getDumpConfigInterval() {
        return dumpConfigInterval;
    }


    private static String getHostAddress() {
        String address = "127.0.0.1";
        try {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            while (en.hasMoreElements()) {
                NetworkInterface ni = en.nextElement();
                Enumeration<InetAddress> ads = ni.getInetAddresses();
                while (ads.hasMoreElements()) {
                    InetAddress ip = ads.nextElement();
                    if (!ip.isLoopbackAddress() && ip.isSiteLocalAddress()) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
        }
        return address;
    }

}
