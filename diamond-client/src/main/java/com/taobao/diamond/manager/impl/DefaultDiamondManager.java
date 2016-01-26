/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.manager.impl;

import com.taobao.diamond.client.DiamondConfigure;
import com.taobao.diamond.client.DiamondSubscriber;
import com.taobao.diamond.client.impl.DefaultSubscriberListener;
import com.taobao.diamond.client.impl.DiamondClientFactory;
import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


/**
 * @author aoqiong
 * Do not use again
 */
@Deprecated
public class DefaultDiamondManager implements DiamondManager {

    private static final Log log = LogFactory.getLog(DefaultDiamondManager.class);

    private DiamondSubscriber diamondSubscriber = null;
    private final List<ManagerListener> managerListeners = new LinkedList<ManagerListener>();
    private final String dataId;
    private final String group;


    public DefaultDiamondManager(String dataId, ManagerListener managerListener) {
        this(null, dataId, managerListener);
    }


    public DefaultDiamondManager(String group, String dataId, ManagerListener managerListener) {
        this.dataId = dataId;
        this.group = group;

        diamondSubscriber = DiamondClientFactory.getSingletonDiamondSubscriber();

        this.managerListeners.add(managerListener);
        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener()).addManagerListeners(this.dataId,
            this.group, this.managerListeners);
        diamondSubscriber.addDataId(this.dataId, this.group);
        diamondSubscriber.start();

    }


    public DefaultDiamondManager(String dataId, List<ManagerListener> managerListenerList) {
        this(null, dataId, managerListenerList);
    }

    public DefaultDiamondManager(String group, String dataId, List<ManagerListener> managerListenerList) {
        this.dataId = dataId;
        this.group = group;

        diamondSubscriber = DiamondClientFactory.getSingletonDiamondSubscriber();

        this.managerListeners.addAll(managerListenerList);
        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener()).addManagerListeners(this.dataId,
            this.group, this.managerListeners);
        diamondSubscriber.addDataId(this.dataId, this.group);
        diamondSubscriber.start();
    }


    public void setManagerListener(ManagerListener managerListener) {
        this.managerListeners.clear();
        this.managerListeners.add(managerListener);

        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener()).removeManagerListeners(this.dataId,
            this.group);
        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener()).addManagerListeners(this.dataId,
            this.group, this.managerListeners);
    }


    public void close() {
        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener()).removeManagerListeners(this.dataId,
            this.group);

        diamondSubscriber.removeDataId(dataId, group);
        if (diamondSubscriber.getDataIds().size() == 0) {
            diamondSubscriber.close();
        }

    }


    public String getConfigureInfomation(long timeout) {
        return diamondSubscriber.getConfigureInformation(this.dataId, this.group, timeout);
    }


    public String getAvailableConfigureInfomation(long timeout) {
        return diamondSubscriber.getAvailableConfigureInformation(dataId, group, timeout);
    }


    public String getAvailableConfigureInfomationFromSnapshot(long timeout) {
        return diamondSubscriber.getAvailableConfigureInformationFromSnapshot(dataId, group, timeout);
    }


    public Properties getAvailablePropertiesConfigureInfomation(long timeout) {
        String configInfo = this.getAvailableConfigureInfomation(timeout);
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(configInfo));
            return properties;
        }
        catch (IOException e) {
            log.warn("Load properties fail:" + configInfo, e);
            throw new RuntimeException("Load properties fail:" + configInfo, e);
        }
    }


    public Properties getAvailablePropertiesConfigureInfomationFromSnapshot(long timeout) {
        String configInfo = this.getAvailableConfigureInfomationFromSnapshot(timeout);
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(configInfo));
            return properties;
        }
        catch (IOException e) {
            log.warn("Load properties fail:" + configInfo, e);
            throw new RuntimeException("Load properties fail:" + configInfo, e);
        }
    }


    public void setManagerListeners(List<ManagerListener> managerListenerList) {
        this.managerListeners.clear();
        this.managerListeners.addAll(managerListenerList);

        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener()).removeManagerListeners(this.dataId,
            this.group);
        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener()).addManagerListeners(this.dataId,
            this.group, this.managerListeners);
    }


    public DiamondConfigure getDiamondConfigure() {
        return diamondSubscriber.getDiamondConfigure();
    }


    public void setDiamondConfigure(DiamondConfigure diamondConfigure) {
        diamondSubscriber.setDiamondConfigure(diamondConfigure);
    }


    public Properties getPropertiesConfigureInfomation(long timeout) {
        String configInfo = this.getConfigureInfomation(timeout);
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(configInfo));
            return properties;
        }
        catch (IOException e) {
            log.warn("Load properties fail:" + configInfo, e);
            throw new RuntimeException("Load properties fail:" + configInfo, e);
        }
    }


    public List<ManagerListener> getManagerListeners() {
        return this.managerListeners;
    }

}
