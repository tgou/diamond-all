/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.client;

import com.taobao.diamond.common.Constants;
import com.taobao.diamond.mockserver.MockServer;

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


/**
 * @author aoqiong
 */
public class DiamondConfigure {

    private volatile int pollingIntervalTime = Constants.POLLING_INTERVAL_TIME;
    private volatile int onceTimeout = Constants.ONCE_TIMEOUT;
    private volatile int receiveWaitTime = Constants.RECV_WAIT_TIMEOUT;

    private volatile List<String> domainNameList = new LinkedList<String>();

    private volatile boolean useFlowControl = true;

    private boolean localFirst = false;

    private int maxHostConnections = 1;
    private boolean connectionStaleCheckingEnabled = true;
    private int maxTotalConnections = 20;
    private int connectionTimeout = Constants.CONN_TIMEOUT;
    private int port = Constants.DEFAULT_PORT;
    private int scheduledThreadPoolSize = 1;
    private int retrieveDataRetryTimes = Integer.MAX_VALUE / 10;

    private String configServerAddress = null;
    private int configServerPort = Constants.DEFAULT_PORT;

    private String filePath;


    public DiamondConfigure() {
        filePath = System.getProperty("user.home") + "/diamond";
        File dir = new File(filePath);
        dir.mkdirs();

        if (!dir.exists()) {
            throw new RuntimeException("Create diamond directory fail: " + filePath);
        }
    }

    public int getMaxHostConnections() {
        return maxHostConnections;
    }

    public void setMaxHostConnections(int maxHostConnections) {
        this.maxHostConnections = maxHostConnections;
    }

    public boolean isConnectionStaleCheckingEnabled() {
        return connectionStaleCheckingEnabled;
    }

    public void setConnectionStaleCheckingEnabled(boolean connectionStaleCheckingEnabled) {
        this.connectionStaleCheckingEnabled = connectionStaleCheckingEnabled;
    }

    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    public void setMaxTotalConnections(int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
    }

    public int getPollingIntervalTime() {
        return pollingIntervalTime;
    }

    public void setPollingIntervalTime(int pollingIntervalTime) {
        if (pollingIntervalTime < Constants.POLLING_INTERVAL_TIME && !MockServer.isTestMode()) {
            return;
        }
        this.pollingIntervalTime = pollingIntervalTime;
    }


    public List<String> getDomainNameList() {
        return domainNameList;
    }

    public void setDomainNameList(List<String> domainNameList) {
        if (null == domainNameList) {
            throw new NullPointerException();
        }
        this.domainNameList = new LinkedList<String>(domainNameList);
    }

    public void addDomainName(String domainName) {
        if (null == domainName) {
            throw new NullPointerException();
        }
        this.domainNameList.add(domainName);
    }

    public void addDomainNames(Collection<String> domainNameList) {
        if (null == domainNameList) {
            throw new NullPointerException();
        }
        this.domainNameList.addAll(domainNameList);
    }


    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public int getOnceTimeout() {
        return onceTimeout;
    }

    public void setOnceTimeout(int onceTimeout) {
        this.onceTimeout = onceTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getReceiveWaitTime() {
        return receiveWaitTime;
    }


    /**
     * the max wait time for one dataID<br>
     * The max wait time < receiveWaitTime + min(connectionTimeout, onceTimeout)
     * suggest set this to OnceTimeout * (DomainName length + 1)
     *
     * @param receiveWaitTime
     */
    public void setReceiveWaitTime(int receiveWaitTime) {
        this.receiveWaitTime = receiveWaitTime;
    }

    public int getScheduledThreadPoolSize() {
        return scheduledThreadPoolSize;
    }

    public void setScheduledThreadPoolSize(int scheduledThreadPoolSize) {
        this.scheduledThreadPoolSize = scheduledThreadPoolSize;
    }

    public boolean isUseFlowControl() {
        return useFlowControl;
    }

    public void setUseFlowControl(boolean useFlowControl) {
        this.useFlowControl = useFlowControl;
    }


    public String getConfigServerAddress() {
        return configServerAddress;
    }


    public void setConfigServerAddress(String configServerAddress) {
        this.configServerAddress = configServerAddress;
    }


    public int getConfigServerPort() {
        return configServerPort;
    }


    public void setConfigServerPort(int configServerPort) {
        this.configServerPort = configServerPort;
    }


    public int getRetrieveDataRetryTimes() {
        return retrieveDataRetryTimes;
    }


    public void setRetrieveDataRetryTimes(int retrieveDataRetryTimes) {
        this.retrieveDataRetryTimes = retrieveDataRetryTimes;
    }


    public boolean isLocalFirst() {
        return localFirst;
    }


    public void setLocalFirst(boolean localFirst) {
        this.localFirst = localFirst;
    }

}
