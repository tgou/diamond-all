/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.client.impl;

import com.taobao.diamond.client.BatchHttpResult;
import com.taobao.diamond.client.DiamondConfigure;
import com.taobao.diamond.client.DiamondSubscriber;
import com.taobao.diamond.client.SubscriberListener;
import com.taobao.diamond.client.processor.LocalConfigInfoProcessor;
import com.taobao.diamond.client.processor.ServerAddressProcessor;
import com.taobao.diamond.client.processor.SnapshotConfigInfoProcessor;
import com.taobao.diamond.common.Constants;
import com.taobao.diamond.configinfo.CacheData;
import com.taobao.diamond.configinfo.ConfigureInfomation;
import com.taobao.diamond.domain.ConfigInfoEx;
import com.taobao.diamond.md5.MD5;
import com.taobao.diamond.mockserver.MockServer;
import com.taobao.diamond.utils.JSONUtils;
import com.taobao.diamond.utils.LoggerInit;
import com.taobao.diamond.utils.SimpleCache;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static com.taobao.diamond.common.Constants.LINE_SEPARATOR;
import static com.taobao.diamond.common.Constants.WORD_SEPARATOR;


/**
 * @author aoqiong
 */
class DefaultDiamondSubscriber implements DiamondSubscriber {
    // 本地文件监视目录
    private static final String DATA_DIR = "data";
    // 上一次正确配置的镜像目录
    private static final String SNAPSHOT_DIR = "snapshot";

    private static final Log log = LogFactory.getLog(DefaultDiamondSubscriber.class);

    private static final int SC_OK = 200;

    private static final int SC_NOT_MODIFIED = 304;

    private static final int SC_NOT_FOUND = 404;

    private static final int SC_SERVICE_UNAVAILABLE = 503;

    static {
        try {
            LoggerInit.initLogFromBizLog();
        } catch (Throwable t) {
        }
    }

    private final Log dataLog = LogFactory.getLog(LoggerInit.LOG_NAME_CONFIG_DATA);

    private final ConcurrentHashMap<String/* DataID */, ConcurrentHashMap<String/* Group */, CacheData>> cache =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, CacheData>>();
    private final LocalConfigInfoProcessor localConfigInfoProcessor = new LocalConfigInfoProcessor();
    private final SimpleCache<String> contentCache = new SimpleCache<String>();
    private final AtomicInteger domainNamePos = new AtomicInteger(0);
    private volatile SubscriberListener subscriberListener = null;
    private volatile DiamondConfigure diamondConfigure;
    private ScheduledExecutorService scheduledExecutor = null;
    private SnapshotConfigInfoProcessor snapshotConfigInfoProcessor;
    private ServerAddressProcessor serverAddressProcessor = null;
    private volatile boolean isRun = false;

    private HttpClient httpClient = null;

    private volatile boolean bFirstCheck = true;


    public DefaultDiamondSubscriber(SubscriberListener subscriberListener) {
        this.subscriberListener = subscriberListener;
        this.diamondConfigure = new DiamondConfigure();
    }

    public synchronized void start() {
        if (isRun) {
            return;
        }

        if (null == scheduledExecutor || scheduledExecutor.isTerminated()) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        }

        localConfigInfoProcessor.start(this.diamondConfigure.getFilePath() + "/" + DATA_DIR);
        serverAddressProcessor = new ServerAddressProcessor(this.diamondConfigure, this.scheduledExecutor);
        serverAddressProcessor.start();

        this.snapshotConfigInfoProcessor = new SnapshotConfigInfoProcessor(this.diamondConfigure.getFilePath() + "/" + SNAPSHOT_DIR);
        randomDomainNamePos();
        initHttpClient();

        isRun = true;

        if (log.isInfoEnabled()) {
            log.info("Current using domains:" + this.diamondConfigure.getDomainNameList());
        }

        if (MockServer.isTestMode()) {
            bFirstCheck = false;
        } else {
            this.diamondConfigure.setPollingIntervalTime(Constants.POLLING_INTERVAL_TIME);
        }
        rotateCheckConfigInfo();

        addShutdownHook();
    }


    private void randomDomainNamePos() {
        Random rand = new Random();
        List<String> domainList = this.diamondConfigure.getDomainNameList();
        if (!domainList.isEmpty()) {
            this.domainNamePos.set(rand.nextInt(domainList.size()));
        }
    }


    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                close();
            }

        });
    }


    protected void initHttpClient() {
        if (MockServer.isTestMode()) {
            return;
        }
        HostConfiguration hostConfiguration = new HostConfiguration();
        hostConfiguration.setHost(diamondConfigure.getDomainNameList().get(this.domainNamePos.get()), diamondConfigure.getPort());

        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        connectionManager.closeIdleConnections(diamondConfigure.getPollingIntervalTime() * 4000);

        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setStaleCheckingEnabled(diamondConfigure.isConnectionStaleCheckingEnabled());
        params.setMaxConnectionsPerHost(hostConfiguration, diamondConfigure.getMaxHostConnections());
        params.setMaxTotalConnections(diamondConfigure.getMaxTotalConnections());
        params.setConnectionTimeout(diamondConfigure.getConnectionTimeout());
        params.setSoTimeout(60 * 1000);

        connectionManager.setParams(params);
        httpClient = new HttpClient(connectionManager);
        httpClient.setHostConfiguration(hostConfiguration);
    }


    /**
     * for test, do not use
     *
     * @param pos
     */
    void setDomainNamesPos(int pos) {
        this.domainNamePos.set(pos);
    }

    private void rotateCheckConfigInfo() {
        scheduledExecutor.schedule(new Runnable() {
            public void run() {
                if (!isRun) {
                    log.warn("DiamondSubscriber is not running. so quit.");
                    return;
                }
                try {
                    checkLocalConfigInfo();
                    checkDiamondServerConfigInfo();
                    checkSnapshot();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("rotateCheckConfigInfo error:", e);
                } finally {
                    rotateCheckConfigInfo();
                }
            }

        }, bFirstCheck ? 60 : diamondConfigure.getPollingIntervalTime(), TimeUnit.SECONDS);
        bFirstCheck = false;
    }

    private void receiveConfigInfo(final CacheData cacheData) {
        scheduledExecutor.execute(new Runnable() {
            public void run() {
                if (!isRun) {
                    log.warn("DiamondSubscriber is not running. so quit.");
                    return;
                }

                try {
                    String configInfo = getConfigureInformation(cacheData.getDataId(), cacheData.getGroup(),
                            diamondConfigure.getReceiveWaitTime(), true);
                    if (null == configInfo) {
                        return;
                    }

                    if (null == subscriberListener) {
                        log.warn("null == subscriberListener");
                        return;
                    }

                    popConfigInfo(cacheData, configInfo);
                } catch (Exception e) {
                    log.error("receiveConfigInfo error:", e);
                }
            }
        });
    }


    private void checkSnapshot() {
        for (Entry<String, ConcurrentHashMap<String, CacheData>> cacheDatasEntry : cache.entrySet()) {
            ConcurrentHashMap<String, CacheData> cacheDatas = cacheDatasEntry.getValue();
            if (null == cacheDatas) {
                continue;
            }
            for (Entry<String, CacheData> cacheDataEntry : cacheDatas.entrySet()) {
                final CacheData cacheData = cacheDataEntry.getValue();
                if (!cacheData.isUseLocalConfigInfo() && cacheData.getFetchCount() == 0) {
                    String configInfo = getSnapshotConfigInformation(cacheData.getDataId(), cacheData.getGroup());
                    if (configInfo != null) {
                        popConfigInfo(cacheData, configInfo);
                    }
                }
            }
        }
    }


    private void checkDiamondServerConfigInfo() {
        Set<String> updateDataIdGroupPairs = checkUpdateDataIds(diamondConfigure.getReceiveWaitTime());
        if (null == updateDataIdGroupPairs || updateDataIdGroupPairs.size() == 0) {
            log.debug("DataID not changed.");
            return;
        }
        for (String freshDataIdGroupPair : updateDataIdGroupPairs) {
            int middleIndex = freshDataIdGroupPair.indexOf(WORD_SEPARATOR);
            if (middleIndex == -1)
                continue;
            String freshDataId = freshDataIdGroupPair.substring(0, middleIndex);
            String freshGroup = freshDataIdGroupPair.substring(middleIndex + 1);

            ConcurrentHashMap<String, CacheData> cacheDatas = cache.get(freshDataId);
            if (null == cacheDatas) {
                continue;
            }
            CacheData cacheData = cacheDatas.get(freshGroup);
            if (null == cacheData) {
                continue;
            }
            receiveConfigInfo(cacheData);
        }
    }


    private void checkLocalConfigInfo() {
        for (Entry<String/* dataId */, ConcurrentHashMap<String/* group */, CacheData>> cacheDatasEntry : cache
                .entrySet()) {
            ConcurrentHashMap<String, CacheData> cacheDatas = cacheDatasEntry.getValue();
            if (null == cacheDatas) {
                continue;
            }
            for (Entry<String, CacheData> cacheDataEntry : cacheDatas.entrySet()) {
                final CacheData cacheData = cacheDataEntry.getValue();
                try {
                    String configInfo = getLocalConfigureInfomation(cacheData);
                    if (null != configInfo) {
                        if (log.isInfoEnabled()) {
                            log.info("Read local configure, dataId:" + cacheData.getDataId() + ", group:" + cacheData.getGroup());
                        }
                        popConfigInfo(cacheData, configInfo);
                        continue;
                    }
                    if (cacheData.isUseLocalConfigInfo()) {
                        continue;
                    }
                } catch (Exception e) {
                    log.error("checkLocalConfigInfo error:", e);
                }
            }
        }
    }

    void popConfigInfo(final CacheData cacheData, final String configInfo) {
        final ConfigureInfomation configureInfomation = new ConfigureInfomation();
        configureInfomation.setConfigureInfomation(configInfo);
        final String dataId = cacheData.getDataId();
        final String group = cacheData.getGroup();
        configureInfomation.setDataId(dataId);
        configureInfomation.setGroup(group);
        cacheData.incrementFetchCountAndGet();
        if (null != this.subscriberListener.getExecutor()) {
            this.subscriberListener.getExecutor().execute(new Runnable() {
                public void run() {
                    try {
                        subscriberListener.receiveConfigInfo(configureInfomation);
                        saveSnapshot(dataId, group, configInfo);
                    } catch (Throwable t) {
                        log.error("popConfigInfo listener receiveConfigInfo error: group=" + group + ", dataId=" + dataId, t);
                    }
                }
            });
        } else {
            try {
                subscriberListener.receiveConfigInfo(configureInfomation);
                saveSnapshot(dataId, group, configInfo);
            } catch (Throwable t) {
                log.error("popConfigInfo listener receiveConfigInfo error: group=" + group + ", dataId=" + dataId, t);
            }
        }
    }


    public synchronized void close() {
        if (!isRun) {
            return;
        }
        log.warn("Close DiamondSubscriber begin.");

        localConfigInfoProcessor.stop();
        serverAddressProcessor.stop();
        isRun = false;
        scheduledExecutor.shutdown();
        cache.clear();

        log.warn("Close DiamondSubscriber end.");
    }

    long getOnceTimeOut(long waitTime, long timeout) {
        long onceTimeOut = this.diamondConfigure.getOnceTimeout();
        long remainTime = timeout - waitTime;
        if (onceTimeOut > remainTime) {
            onceTimeOut = remainTime;
        }
        return onceTimeOut;
    }


    public String getLocalConfigureInfomation(CacheData cacheData) throws IOException {
        if (!isRun) {
            throw new RuntimeException("DiamondSubscriber is not running, can't fetch local ConfigureInfomation");
        }
        return localConfigInfoProcessor.getLocalConfigureInfomation(cacheData, false);
    }


    public String getConfigureInformation(String dataId, long timeout) {
        return getConfigureInformation(dataId, null, timeout);
    }


    public String getConfigureInformation(String dataId, String group, long timeout) {
        if (null == group) {
            group = Constants.DEFAULT_GROUP;
        }
        CacheData cacheData = getCacheData(dataId, group);
        try {
            String localConfig = localConfigInfoProcessor.getLocalConfigureInfomation(cacheData, true);
            if (localConfig != null) {
                cacheData.incrementFetchCountAndGet();
                saveSnapshot(dataId, group, localConfig);

                return localConfig;
            }
        } catch (IOException e) {
            log.error("getLocalConfigureInfomation error:", e);
        }
        String result = getConfigureInformation(dataId, group, timeout, false);
        if (result != null) {
            saveSnapshot(dataId, group, result);
            cacheData.incrementFetchCountAndGet();
        }
        return result;
    }


    private void saveSnapshot(String dataId, String group, String config) {
        if (config != null) {
            try {
                this.snapshotConfigInfoProcessor.saveSnapshot(dataId, group, config);
            } catch (IOException e) {
                log.error("saveSnapshot error,dataId=" + dataId + ",group=" + group, e);
            }
        }
    }


    public String getAvailableConfigureInformation(String dataId, String group, long timeout) {
        try {
            String result = getConfigureInformation(dataId, group, timeout);

            if (result != null && result.length() > 0) {
                return result;
            }
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        }

        if (MockServer.isTestMode()) {
            return null;
        }
        return getSnapshotConfigInformation(dataId, group);
    }

    public String getFromLocalAndSnapshot(String dataId, String group, long timeout) {
        try {
            String result = getConfigureInfomationFromLocal(dataId, group, timeout);
            if (result != null && result.length() > 0) {
                return result;
            }
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        }

        if (MockServer.isTestMode()) {
            return null;
        }
        return getSnapshotConfigInformation(dataId, group);
    }

    public String getConfigureInfomationFromLocal(String dataId, String group, long timeout) {
        if (null == group) {
            group = Constants.DEFAULT_GROUP;
        }
        CacheData cacheData = getCacheData(dataId, group);
        try {
            String localConfig = localConfigInfoProcessor.getLocalConfigureInfomation(cacheData, true);
            if (localConfig != null) {
                cacheData.incrementFetchCountAndGet();
                saveSnapshot(dataId, group, localConfig);

                return localConfig;
            }
        } catch (IOException e) {
            log.error("getLocalConfigureInfomation error", e);
        }

        return null;
    }


    public String getAvailableConfigureInformationFromSnapshot(String dataId, String group, long timeout) {
        String result = getSnapshotConfigInformation(dataId, group);
        if (!StringUtils.isBlank(result)) {
            return result;
        }
        return getConfigureInformation(dataId, group, timeout);
    }


    private String getSnapshotConfigInformation(String dataId, String group) {
        if (group == null) {
            group = Constants.DEFAULT_GROUP;
        }
        try {
            CacheData cacheData = getCacheData(dataId, group);
            String config = this.snapshotConfigInfoProcessor.getConfigInfomation(dataId, group);
            if (config != null && cacheData != null) {
                cacheData.incrementFetchCountAndGet();
            }
            return config;
        } catch (Exception e) {
            log.error("getSnapshotConfigInformation error dataId=" + dataId + ",group=" + group, e);
            return null;
        }
    }

    String getConfigureInformation(String dataId, String group, long timeout, boolean skipContentCache) {
        start();
        if (!isRun) {
            throw new RuntimeException("DiamondSubscriber is not running, so can't fetch from ConfigureInformation");
        }
        if (null == group) {
            group = Constants.DEFAULT_GROUP;
        }
        // =======================ʹ�ò���ģʽ=======================
        if (MockServer.isTestMode()) {
            return MockServer.getConfigInfo(dataId, group);
        }

        if (!skipContentCache) {
            String key = makeCacheKey(dataId, group);
            String content = contentCache.get(key);
            if (content != null) {
                return content;
            }
        }

        long waitTime = 0;

        String uri = getUriString(dataId, group);
        if (log.isInfoEnabled()) {
            log.info(uri);
        }

        CacheData cacheData = getCacheData(dataId, group);

        int retryTimes = this.getDiamondConfigure().getRetrieveDataRetryTimes();
        log.info("Retry times is " + retryTimes);
        int tryCount = 0;

        while (0 == timeout || timeout > waitTime) {
            tryCount++;
            if (tryCount > retryTimes + 1) {
                log.warn("Retry time reach the limit, so break");
                break;
            }
            log.info("Fetch config " + tryCount + "times, waitTime:" + waitTime);

            long onceTimeOut = getOnceTimeOut(waitTime, timeout);
            waitTime += onceTimeOut;

            HttpMethod httpMethod = new GetMethod(uri);

            configureHttpMethod(skipContentCache, cacheData, onceTimeOut, httpMethod);

            try {
                int httpStatus = httpClient.executeMethod(httpMethod);

                switch (httpStatus) {

                    case SC_OK: {
                        String result = getSuccess(dataId, group, cacheData, httpMethod);
                        return result;
                    }

                    case SC_NOT_MODIFIED: {
                        String result = getNotModified(dataId, cacheData, httpMethod);
                        return result;
                    }

                    case SC_NOT_FOUND: {
                        log.warn("DataID:" + dataId + "not found");
                        cacheData.setMd5(Constants.NULL);
                        this.snapshotConfigInfoProcessor.removeSnapshot(dataId, group);
                        return null;
                    }

                    case SC_SERVICE_UNAVAILABLE: {
                        rotateToNextDomain();
                    }
                    break;

                    default: {
                        log.warn("HTTP State: " + httpStatus + ":" + httpClient.getState());
                        rotateToNextDomain();
                    }
                }
            } catch (HttpException e) {
                log.error("Fetch config HttpException", e);
                rotateToNextDomain();
            } catch (IOException e) {
                log.error("Fetch config IOException", e);
                rotateToNextDomain();
            } catch (Exception e) {
                log.error("Unknown Exception", e);
                rotateToNextDomain();
            } finally {
                httpMethod.releaseConnection();
            }
        }
        throw new RuntimeException("Fetch config timeout, DataID=" + dataId + ", Group=" + group + ",timeout=" + timeout);
    }


    private CacheData getCacheData(String dataId, String group) {
        CacheData cacheData = null;
        ConcurrentHashMap<String, CacheData> cacheDatas = this.cache.get(dataId);
        if (null != cacheDatas) {
            cacheData = cacheDatas.get(group);
        }
        if (null == cacheData) {
            cacheData = new CacheData(dataId, group);
            ConcurrentHashMap<String, CacheData> newCacheDatas = new ConcurrentHashMap<String, CacheData>();
            ConcurrentHashMap<String, CacheData> oldCacheDatas = this.cache.putIfAbsent(dataId, newCacheDatas);
            if (null == oldCacheDatas) {
                oldCacheDatas = newCacheDatas;
            }
            if (null != oldCacheDatas.putIfAbsent(group, cacheData)) {
                cacheData = oldCacheDatas.get(group);
            }
        }
        return cacheData;
    }

    private String getNotModified(String dataId, CacheData cacheData, HttpMethod httpMethod) {
        Header md5Header = httpMethod.getResponseHeader(Constants.CONTENT_MD5);
        if (null == md5Header) {
            throw new RuntimeException("RP_NO_CHANGE response not contain MD5");
        }
        String md5 = md5Header.getValue();
        if (!cacheData.getMd5().equals(md5)) {
            String lastMd5 = cacheData.getMd5();
            cacheData.setMd5(Constants.NULL);
            cacheData.setLastModifiedHeader(Constants.NULL);

            throw new RuntimeException("MD5 verify error,DataID:[" + dataId + "]MD5 last:[" + lastMd5 + "]MD5 current:[" + md5 + "]");
        }

        cacheData.setMd5(md5);
        changeSpacingInterval(httpMethod);
        if (log.isInfoEnabled()) {
            log.info("DataId: " + dataId + ",not changed");
        }
        return null;
    }

    private String getSuccess(String dataId, String group, CacheData cacheData, HttpMethod httpMethod) {
        String configInfo = Constants.NULL;
        configInfo = getContent(httpMethod);
        if (null == configInfo) {
            throw new RuntimeException("RP_OK configInfo is null");
        }

        Header md5Header = httpMethod.getResponseHeader(Constants.CONTENT_MD5);
        if (null == md5Header) {
            throw new RuntimeException("RP_OK not contain MD5, " + configInfo);
        }
        String md5 = md5Header.getValue();
        if (!checkContent(configInfo, md5)) {
            throw new RuntimeException("MD5 verify error,DataID:[" + dataId + "]ConfigInfo:[" + configInfo + "]MD5:[" + md5 + "]");
        }

        Header lastModifiedHeader = httpMethod.getResponseHeader(Constants.LAST_MODIFIED);
        if (null == lastModifiedHeader) {
            throw new RuntimeException("RP_OK result not contain lastModifiedHeader");
        }
        String lastModified = lastModifiedHeader.getValue();

        cacheData.setMd5(md5);
        cacheData.setLastModifiedHeader(lastModified);

        changeSpacingInterval(httpMethod);

        String key = makeCacheKey(dataId, group);
        contentCache.put(key, configInfo);

        StringBuilder buf = new StringBuilder();
        buf.append("dataId=").append(dataId);
        buf.append(" ,group=").append(group);
        buf.append(" ,content=").append(configInfo);
        dataLog.info(buf.toString());

        return configInfo;
    }


    private void configureHttpMethod(boolean skipContentCache, CacheData cacheData, long onceTimeOut,
                                     HttpMethod httpMethod) {
        if (skipContentCache && null != cacheData) {
            if (null != cacheData.getLastModifiedHeader() && Constants.NULL != cacheData.getLastModifiedHeader()) {
                httpMethod.addRequestHeader(Constants.IF_MODIFIED_SINCE, cacheData.getLastModifiedHeader());
            }
            if (null != cacheData.getMd5() && Constants.NULL != cacheData.getMd5()) {
                httpMethod.addRequestHeader(Constants.CONTENT_MD5, cacheData.getMd5());
            }
        }

        httpMethod.addRequestHeader(Constants.ACCEPT_ENCODING, "gzip,deflate");

        HttpMethodParams params = new HttpMethodParams();
        params.setSoTimeout((int) onceTimeOut);
        httpMethod.setParams(params);
        httpClient.getHostConfiguration().setHost(diamondConfigure.getDomainNameList().get(this.domainNamePos.get()), diamondConfigure.getPort());
    }


    private String makeCacheKey(String dataId, String group) {
        String key = dataId + "-" + group;
        return key;
    }

    Set<String> checkUpdateDataIds(long timeout) {
        if (!isRun) {
            throw new RuntimeException("DiamondSubscriber is not running. checkUpdateDataIds return.");
        }
        if (MockServer.isTestMode()) {
            return testData();
        }
        long waitTime = 0;

        String probeUpdateString = getProbeUpdateString();
        if (StringUtils.isBlank(probeUpdateString)) {
            return null;
        }

        while (0 == timeout || timeout > waitTime) {
            long onceTimeOut = getOnceTimeOut(waitTime, timeout);
            waitTime += onceTimeOut;

            PostMethod postMethod = new PostMethod(Constants.HTTP_URI_FILE);

            postMethod.addParameter(Constants.PROBE_MODIFY_REQUEST, probeUpdateString);

            HttpMethodParams params = new HttpMethodParams();
            params.setSoTimeout((int) onceTimeOut);
            postMethod.setParams(params);

            try {
                httpClient.getHostConfiguration()
                        .setHost(diamondConfigure.getDomainNameList().get(this.domainNamePos.get()),
                                this.diamondConfigure.getPort());

                int httpStatus = httpClient.executeMethod(postMethod);

                switch (httpStatus) {
                    case SC_OK: {
                        Set<String> result = getUpdateDataIds(postMethod);
                        return result;
                    }

                    case SC_SERVICE_UNAVAILABLE: {
                        rotateToNextDomain();
                    }
                    break;

                    default: {
                        log.warn("checkUpdateDataIds HTTP State: " + httpStatus);
                        rotateToNextDomain();
                    }
                }
            } catch (HttpException e) {
                log.error("checkUpdateDataIds HttpException", e);
                rotateToNextDomain();
            } catch (IOException e) {
                log.error("checkUpdateDataIds IOException", e);
                rotateToNextDomain();
            } catch (Exception e) {
                log.error("checkUpdateDataIds Unknown Exception", e);
                rotateToNextDomain();
            } finally {
                postMethod.releaseConnection();
            }
        }
        throw new RuntimeException("checkUpdateDataIds timeout "
                + diamondConfigure.getDomainNameList().get(this.domainNamePos.get()) + ", timeout=" + timeout);
    }


    private Set<String> testData() {
        Set<String> dataIdList = new HashSet<String>();
        for (String dataId : this.cache.keySet()) {
            ConcurrentHashMap<String, CacheData> cacheDatas = this.cache.get(dataId);
            for (String group : cacheDatas.keySet()) {
                if (null != MockServer.getUpdateConfigInfo(dataId, group)) {
                    dataIdList.add(dataId + WORD_SEPARATOR + group);
                }
            }
        }
        return dataIdList;
    }

    private String getProbeUpdateString() {
        StringBuilder probeModifyBuilder = new StringBuilder();
        for (Entry<String, ConcurrentHashMap<String, CacheData>> cacheDatasEntry : this.cache.entrySet()) {
            String dataId = cacheDatasEntry.getKey();
            ConcurrentHashMap<String, CacheData> cacheDatas = cacheDatasEntry.getValue();
            if (null == cacheDatas) {
                continue;
            }
            for (Entry<String, CacheData> cacheDataEntry : cacheDatas.entrySet()) {
                final CacheData data = cacheDataEntry.getValue();
                if (!data.isUseLocalConfigInfo()) {
                    probeModifyBuilder.append(dataId).append(WORD_SEPARATOR);

                    if (null != cacheDataEntry.getValue().getGroup()
                            && Constants.NULL != cacheDataEntry.getValue().getGroup()) {
                        probeModifyBuilder.append(cacheDataEntry.getValue().getGroup()).append(WORD_SEPARATOR);
                    } else {
                        probeModifyBuilder.append(WORD_SEPARATOR);
                    }

                    if (null != cacheDataEntry.getValue().getMd5()
                            && Constants.NULL != cacheDataEntry.getValue().getMd5()) {
                        probeModifyBuilder.append(cacheDataEntry.getValue().getMd5()).append(LINE_SEPARATOR);
                    } else {
                        probeModifyBuilder.append(LINE_SEPARATOR);
                    }
                }
            }
        }
        String probeModifyString = probeModifyBuilder.toString();
        return probeModifyString;
    }


    synchronized void rotateToNextDomain() {
        int domainNameCount = diamondConfigure.getDomainNameList().size();
        int index = domainNamePos.incrementAndGet();
        if (index < 0) {
            index = -index;
        }
        if (domainNameCount == 0) {
            log.error("rotateToNextDomain domainNameCount is 0, administrator should resolve this");
            return;
        }
        domainNamePos.set(index % domainNameCount);
        if (diamondConfigure.getDomainNameList().size() > 0)
            log.warn("Rotate domain name to " + diamondConfigure.getDomainNameList().get(domainNamePos.get()));
    }

    String getUriString(String dataId, String group) {
        StringBuilder uriBuilder = new StringBuilder();
        uriBuilder.append(Constants.HTTP_URI_FILE);
        uriBuilder.append("?");
        uriBuilder.append(Constants.DATAID).append("=").append(dataId);
        if (null != group) {
            uriBuilder.append("&");
            uriBuilder.append(Constants.GROUP).append("=").append(group);
        }
        return uriBuilder.toString();
    }

    void changeSpacingInterval(HttpMethod httpMethod) {
        Header[] spacingIntervalHeaders = httpMethod.getResponseHeaders(Constants.SPACING_INTERVAL);
        if (spacingIntervalHeaders.length >= 1) {
            try {
                diamondConfigure.setPollingIntervalTime(Integer.parseInt(spacingIntervalHeaders[0].getValue()));
            } catch (RuntimeException e) {
                log.error("�����´μ��ʱ��ʧ��", e);
            }
        }
    }

    String getContent(HttpMethod httpMethod) {
        StringBuilder contentBuilder = new StringBuilder();
        if (isZipContent(httpMethod)) {
            InputStream is = null;
            GZIPInputStream gzin = null;
            InputStreamReader isr = null;
            BufferedReader br = null;
            try {
                is = httpMethod.getResponseBodyAsStream();
                gzin = new GZIPInputStream(is);
                isr = new InputStreamReader(gzin, ((HttpMethodBase) httpMethod).getResponseCharSet()); // ���ö�ȡ���ı����ʽ���Զ������
                br = new BufferedReader(isr);
                char[] buffer = new char[4096];
                int readlen = -1;
                while ((readlen = br.read(buffer, 0, 4096)) != -1) {
                    contentBuilder.append(buffer, 0, readlen);
                }
            } catch (Exception e) {
                log.error("Unzip fail", e);
            } finally {
                try {
                    br.close();
                } catch (Exception e1) {
                    // ignore
                }
                try {
                    isr.close();
                } catch (Exception e1) {
                    // ignore
                }
                try {
                    gzin.close();
                } catch (Exception e1) {
                    // ignore
                }
                try {
                    is.close();
                } catch (Exception e1) {
                    // ignore
                }
            }
        } else {
            String content = null;
            try {
                content = httpMethod.getResponseBodyAsString();
            } catch (Exception e) {
                log.error("Fetch config error:", e);
            }
            if (null == content) {
                return null;
            }
            contentBuilder.append(content);
        }
        return contentBuilder.toString();
    }


    Set<String> getUpdateDataIdsInBody(HttpMethod httpMethod) {
        Set<String> modifiedDataIdSet = new HashSet<String>();
        try {
            String modifiedDataIdsString = httpMethod.getResponseBodyAsString();
            return convertStringToSet(modifiedDataIdsString);
        } catch (Exception e) {

        }
        return modifiedDataIdSet;

    }


    Set<String> getUpdateDataIds(HttpMethod httpMethod) {
        return getUpdateDataIdsInBody(httpMethod);
    }


    private Set<String> convertStringToSet(String modifiedDataIdsString) {

        if (null == modifiedDataIdsString || "".equals(modifiedDataIdsString)) {
            return null;
        }

        Set<String> modifiedDataIdSet = new HashSet<String>();

        try {
            modifiedDataIdsString = URLDecoder.decode(modifiedDataIdsString, "UTF-8");
        } catch (Exception e) {
            log.error("Decode modifiedDataIdsString error:", e);
        }

        if (log.isInfoEnabled() && modifiedDataIdsString != null) {
            if (modifiedDataIdsString.startsWith("OK")) {
                log.debug("modifiedDataIdsString is " + modifiedDataIdsString);
            } else {
                log.info("modifiedDataIdsString changed:" + modifiedDataIdsString);
            }
        }

        final String[] modifiedDataIdStrings = modifiedDataIdsString.split(LINE_SEPARATOR);
        for (String modifiedDataIdString : modifiedDataIdStrings) {
            if (!"".equals(modifiedDataIdString)) {
                modifiedDataIdSet.add(modifiedDataIdString);
            }
        }
        return modifiedDataIdSet;
    }


    boolean checkContent(String configInfo, String md5) {
        String realMd5 = MD5.getInstance().getMD5String(configInfo);
        return realMd5 == null ? md5 == null : realMd5.equals(md5);
    }

    boolean isZipContent(HttpMethod httpMethod) {
        if (null != httpMethod.getResponseHeader(Constants.CONTENT_ENCODING)) {
            String acceptEncoding = httpMethod.getResponseHeader(Constants.CONTENT_ENCODING).getValue();
            if (acceptEncoding.toLowerCase().indexOf("gzip") > -1) {
                return true;
            }
        }
        return false;
    }

    public SubscriberListener getSubscriberListener() {
        return this.subscriberListener;
    }

    public void setSubscriberListener(SubscriberListener subscriberListener) {
        this.subscriberListener = subscriberListener;
    }

    public void addDataId(String dataId, String group) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("diamond client start:" + formatter.format(new Date(System.currentTimeMillis())));
        if (null == group) {
            group = Constants.DEFAULT_GROUP;
        }

        ConcurrentHashMap<String, CacheData> cacheDatas = this.cache.get(dataId);
        if (null == cacheDatas) {
            ConcurrentHashMap<String, CacheData> newCacheDatas = new ConcurrentHashMap<String, CacheData>();
            ConcurrentHashMap<String, CacheData> oldCacheDatas = this.cache.putIfAbsent(dataId, newCacheDatas);
            if (null != oldCacheDatas) {
                cacheDatas = oldCacheDatas;
            } else {
                cacheDatas = newCacheDatas;
            }
        }
        CacheData cacheData = cacheDatas.get(group);
        if (null == cacheData) {
            cacheDatas.putIfAbsent(group, new CacheData(dataId, group));
            if (log.isInfoEnabled()) {
                log.info("Added DataID[" + dataId + "]Group=" + group);
            }
            this.start();
        }
    }


    public void addDataId(String dataId) {
        addDataId(dataId, null);
    }


    public boolean containDataId(String dataId) {
        return containDataId(dataId, null);
    }


    public boolean containDataId(String dataId, String group) {
        if (null == group) {
            group = Constants.DEFAULT_GROUP;
        }
        ConcurrentHashMap<String, CacheData> cacheDatas = this.cache.get(dataId);
        if (null == cacheDatas) {
            return false;
        }
        return cacheDatas.containsKey(group);
    }


    public void clearAllDataIds() {
        this.cache.clear();
    }


    public Set<String> getDataIds() {
        return new HashSet<String>(this.cache.keySet());
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<String, CacheData>> getCache() {
        return cache;
    }


    public void removeDataId(String dataId) {
        removeDataId(dataId, null);
    }


    public synchronized void removeDataId(String dataId, String group) {
        if (null == group) {
            group = Constants.DEFAULT_GROUP;
        }
        ConcurrentHashMap<String, CacheData> cacheDatas = this.cache.get(dataId);
        if (null == cacheDatas) {
            return;
        }
        cacheDatas.remove(group);

        log.warn("Remove DataID[" + dataId + "]Group: " + group);

        if (cacheDatas.size() == 0) {
            this.cache.remove(dataId);
            log.warn("Remove DataID[" + dataId + "]");
        }
    }


    public DiamondConfigure getDiamondConfigure() {
        return this.diamondConfigure;
    }


    public void setDiamondConfigure(DiamondConfigure diamondConfigure) {
        if (!isRun) {
            this.diamondConfigure = diamondConfigure;
        } else {
            copyDiamondConfigure(diamondConfigure);
        }
    }


    private void copyDiamondConfigure(DiamondConfigure diamondConfigure) {
        // TODO which config can dynamic update?
    }

    @Override
    public BatchHttpResult getConfigureInformationBatch(List<String> dataIds, String group, int timeout) {
        if (dataIds == null) {
            log.error("dataId list cannot be null,group=" + group);
            return new BatchHttpResult(HttpStatus.SC_BAD_REQUEST);
        }
        if (group == null) {
            group = Constants.DEFAULT_GROUP;
        }

        StringBuilder dataIdBuilder = new StringBuilder();
        for (String dataId : dataIds) {
            dataIdBuilder.append(dataId).append(Constants.LINE_SEPARATOR);
        }
        String dataIdStr = dataIdBuilder.toString();

        PostMethod post = new PostMethod(Constants.HTTP_URI_FILE_BATCH);
        post.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);

        BatchHttpResult response = null;
        try {
            NameValuePair dataIdValue = new NameValuePair("dataIds", dataIdStr);
            NameValuePair groupValue = new NameValuePair("group", group);

            post.setRequestBody(new NameValuePair[]{dataIdValue, groupValue});

            httpClient.getHostConfiguration()
                    .setHost(diamondConfigure.getDomainNameList().get(this.domainNamePos.get()),
                            this.diamondConfigure.getPort());
            int status = httpClient.executeMethod(post);
            String responseMsg = post.getResponseBodyAsString();

            if (status == HttpStatus.SC_OK) {
                String json = null;
                try {
                    json = responseMsg;

                    List<ConfigInfoEx> configInfoExList = new LinkedList<ConfigInfoEx>();
                    Object resultObj = JSONUtils.deserializeObject(json, new TypeReference<List<ConfigInfoEx>>() {
                    });
                    if (!(resultObj instanceof List<?>)) {
                        throw new RuntimeException("batch query deserialize type error, not list, json=" + json);
                    }
                    List<ConfigInfoEx> resultList = (List<ConfigInfoEx>) resultObj;
                    for (ConfigInfoEx configInfoEx : resultList) {
                        configInfoExList.add(configInfoEx);
                    }

                    response = new BatchHttpResult(configInfoExList);
                    log.info("batch query success,dataIds=" + dataIdStr + ",group="
                            + group + ",json=" + json);
                } catch (Exception e) {
                    response = new BatchHttpResult(Constants.BATCH_OP_ERROR);
                    log.error("batch query deserialize error,dataIdStr=" + dataIdStr
                            + ",group=" + group + ",json=" + json, e);
                }

            } else if (status == HttpStatus.SC_REQUEST_TIMEOUT) {
                response = new BatchHttpResult(HttpStatus.SC_REQUEST_TIMEOUT);
                log.error("batch query timeout, socket timeout(ms):" + timeout + ",dataIds=" + dataIdStr + ",group=" + group);
            } else {
                response = new BatchHttpResult(status);
                log.error("batch query fail, status:" + status + ", response:" + responseMsg + ",dataIds=" + dataIdStr + ",group=" + group);
            }
        } catch (HttpException e) {
            response = new BatchHttpResult(Constants.BATCH_HTTP_EXCEPTION);
            log.error("batch query http exception,dataIds=" + dataIdStr + ",group=" + group, e);
        } catch (IOException e) {
            response = new BatchHttpResult(Constants.BATCH_IO_EXCEPTION);
            log.error("batch query io exception, dataIds=" + dataIdStr + ",group=" + group, e);
        } finally {
            post.releaseConnection();
        }

        return response;
    }
}
