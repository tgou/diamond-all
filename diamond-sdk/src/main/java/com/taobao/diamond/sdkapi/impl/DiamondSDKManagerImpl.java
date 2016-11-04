/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.sdkapi.impl;

import com.taobao.diamond.common.Constants;
import com.taobao.diamond.domain.*;
import com.taobao.diamond.sdkapi.DiamondSDKManager;
import com.taobao.diamond.util.PatternUtils;
import com.taobao.diamond.util.RandomDiamondUtils;
import com.taobao.diamond.utils.JSONUtils;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.type.TypeReference;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;


/**
 *
 * @filename DiamondSDKManagerImpl.java
 * @author libinbin.pt
 * @datetime 2010-7-16 04:00:19
 */
public class DiamondSDKManagerImpl implements DiamondSDKManager {

    private static final Log log = LogFactory.getLog("diamondSdkLog");

    private Map<String, DiamondSDKConf> diamondSDKConfMaps;

    private static final int DEFAULT_CONNECTION_TIMEOUT = 1000;

    private static final int DEFAULT_REQUIRE_TIMEOUT = 2000;

    private final int connectionTimeout;

    private final int requireTimeout;

    public DiamondSDKManagerImpl() {
        this(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_REQUIRE_TIMEOUT, null);
    }

    public DiamondSDKManagerImpl(Map<String, DiamondSDKConf> diamondSDKConfMaps) {
        this(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_REQUIRE_TIMEOUT, diamondSDKConfMaps);
    }

    public DiamondSDKManagerImpl(int connectionTimeout, int requireTimeout) {
        this(connectionTimeout, requireTimeout, null);
    }

    public DiamondSDKManagerImpl(int connectionTimeout, int requireTimeout, Map<String, DiamondSDKConf> diamondSDKConfMaps) throws IllegalArgumentException {
        if (connectionTimeout < 0)
            throw new IllegalArgumentException("connection_time must >= 0!");
        if (requireTimeout < 0)
            throw new IllegalArgumentException("requireTimeout must >= 0!");
        this.connectionTimeout = connectionTimeout;
        this.requireTimeout = requireTimeout;
        this.diamondSDKConfMaps = diamondSDKConfMaps;

        int maxHostConnections = 50;
        MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();

        connectionManager.getParams().setDefaultMaxConnectionsPerHost(maxHostConnections);
        connectionManager.getParams().setStaleCheckingEnabled(true);
        this.client = new HttpClient(connectionManager);
        client.getHttpConnectionManager().getParams().setConnectionTimeout(this.connectionTimeout);
        client.getHttpConnectionManager().getParams().setSoTimeout(60 * 1000);
        client.getParams().setContentCharset("UTF-8");
        log.info("HttpClient create success: connectionTimeout" + this.connectionTimeout);
    }

    public synchronized ContextResult publish(String dataId, String groupName, String context, String serverId) {
        ContextResult response = null;
        if (validate(dataId, groupName, context)) {
            response = this.processPublishByDefinedServerId(dataId, groupName, context, serverId);
            return response;
        }

        response = new ContextResult();
        response.setSuccess(false);
        response.setStatusMsg("dataId,group,content illegal");
        return response;
    }

    public synchronized ContextResult publishAfterModified(String dataId, String groupName, String context,
                                                           String serverId) {

        ContextResult response = null;
        if (validate(dataId, groupName, context)) {
            response = this.processPublishAfterModifiedByDefinedServerId(dataId, groupName, context, serverId);
            return response;
        }
        else {
            response = new ContextResult();
            response.setSuccess(false);
            response.setStatusMsg("dataId,group,content illegal");
            return response;
        }

    }

    public synchronized PageContextResult<ConfigInfo> queryBy(String dataIdPattern, String groupNamePattern,
            String serverId, long currentPage, long sizeOfPerPage) {
        return processQuery(dataIdPattern, groupNamePattern, null, serverId, currentPage, sizeOfPerPage);
    }

    public synchronized PageContextResult<ConfigInfo> queryBy(String dataIdPattern, String groupNamePattern,
            String contentPattern, String serverId, long currentPage, long sizeOfPerPage) {
        return processQuery(dataIdPattern, groupNamePattern, contentPattern, serverId, currentPage, sizeOfPerPage);
    }

    public synchronized ContextResult queryByDataIdAndGroupName(String dataId, String groupName, String serverId) {
        ContextResult result = new ContextResult();
        PageContextResult<ConfigInfo> pageContextResult = processQuery(dataId, groupName, null, serverId, 1, 1);
        result.setStatusMsg(pageContextResult.getStatusMsg());
        result.setSuccess(pageContextResult.isSuccess());
        result.setStatusCode(pageContextResult.getStatusCode());
        if (pageContextResult.isSuccess()) {
            List<ConfigInfo> list = pageContextResult.getDiamondData();
            if (list != null && !list.isEmpty()) {
                ConfigInfo info = list.iterator().next();
                result.setConfigInfo(info);
                result.setReceiveResult(info.getContent());
                result.setStatusCode(pageContextResult.getStatusCode());

            }
        }
        return result;
    }

    private final HttpClient client;

    private ContextResult processPublishByDefinedServerId(String dataId, String groupName, String context,
                                                          String serverId) {
        ContextResult response = new ContextResult();
        if (!login(serverId)) {
            response.setSuccess(false);
            response.setStatusMsg("login fail");
            return response;
        }
        if (log.isDebugEnabled())
            log.debug("processPublishByDefinedServerId(" + dataId + "," + groupName + "," + context + "," + serverId + ")");

        String postUrl = "/admin.do?method=postConfig";
        PostMethod post = new PostMethod(postUrl);
        post.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, requireTimeout);
        try {
            NameValuePair dataId_value = new NameValuePair("dataId", dataId);
            NameValuePair group_value = new NameValuePair("group", groupName);
            NameValuePair content_value = new NameValuePair("content", context);

            post.setRequestBody(new NameValuePair[] { dataId_value, group_value, content_value });
            ConfigInfo configInfo = new ConfigInfo();
            configInfo.setDataId(dataId);
            configInfo.setGroup(groupName);
            configInfo.setContent(context);
            if (log.isDebugEnabled())
                log.debug("Publish ConfigInfo: " + configInfo);
            response.setConfigInfo(configInfo);
            int status = client.executeMethod(post);
            response.setReceiveResult(post.getResponseBodyAsString());
            response.setStatusCode(status);
            log.info("Publish status=" + status + ",response" + post.getResponseBodyAsString());
            if (status == HttpStatus.SC_OK) {
                response.setSuccess(true);
                response.setStatusMsg("Publish success");
                log.info("Publish success, dataId=" + dataId + ",group=" + groupName + ",content=" + context + ",serverId=" + serverId);
            }
            else if (status == HttpStatus.SC_REQUEST_TIMEOUT) {
                response.setSuccess(false);
                response.setStatusMsg("Publish timeout: requireTimeout=" + requireTimeout);
                log.error("Publish timeout: requireTimeout=" + requireTimeout + ", dataId=" + dataId + ",group=" + groupName
                        + ",content=" + context + ",serverId=" + serverId);
            }
            else {
                response.setSuccess(false);
                response.setStatusMsg("Publish fail status=:" + status);
                log.error("Publish fail:result=" + response.getReceiveResult() + ",dataId=" + dataId + ",group=" + groupName
                        + ",content=" + context + ",serverId=" + serverId);
            }
        }
        catch (HttpException e) {
            response.setStatusMsg("Publish HttpException" + e.getMessage());
            log.error("Publish HttpException: dataId=" + dataId + ",group=" + groupName + ",content=" + context
                    + ",serverId=" + serverId, e);
        }
        catch (IOException e) {
            response.setStatusMsg("Publish IOException" + e.getMessage());
            log.error("Publish IOException: dataId=" + dataId + ",group=" + groupName + ",content=" + context
                    + ",serverId=" + serverId, e);
        }
        finally {
            post.releaseConnection();
        }

        return response;
    }


    private ContextResult processPublishAfterModifiedByDefinedServerId(String dataId, String groupName, String context,
                                                                       String serverId) {
        ContextResult response = new ContextResult();
        if (!login(serverId)) {
            response.setSuccess(false);
            response.setStatusMsg("login fail.");
            return response;
        }
        if (log.isDebugEnabled())
            log.debug("processPublishAfterModifiedByDefinedServerId(" + dataId + "," + groupName + "," + context + ","
                    + serverId + ")");
        ContextResult result = null;
        result = queryByDataIdAndGroupName(dataId, groupName, serverId);
        if (null == result || !result.isSuccess()) {
            response.setSuccess(false);
            response.setStatusMsg("publish fail!");
            log.warn("publish fail: dataId=" + dataId + ",group=" + groupName + ",serverId=" + serverId);
            return response;
        }
        else {
            String postUrl = "/admin.do?method=updateConfig";
            PostMethod post = new PostMethod(postUrl);
            post.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, requireTimeout);
            try {
                NameValuePair dataId_value = new NameValuePair("dataId", dataId);
                NameValuePair group_value = new NameValuePair("group", groupName);
                NameValuePair content_value = new NameValuePair("content", context);
                post.setRequestBody(new NameValuePair[] { dataId_value, group_value, content_value });
                ConfigInfo configInfo = new ConfigInfo();
                configInfo.setDataId(dataId);
                configInfo.setGroup(groupName);
                configInfo.setContent(context);
                if (log.isDebugEnabled())
                    log.debug("Publish ConfigInfo: " + configInfo);
                response.setConfigInfo(configInfo);
                int status = client.executeMethod(post);
                response.setReceiveResult(post.getResponseBodyAsString());
                response.setStatusCode(status);
                log.info("Publish status=" + status + ",response" + post.getResponseBodyAsString());
                if (status == HttpStatus.SC_OK) {
                    response.setSuccess(true);
                    response.setStatusMsg("Publish success.");
                    log.info("Publish success.");
                }
                else if (status == HttpStatus.SC_REQUEST_TIMEOUT) {
                    response.setSuccess(false);
                    response.setStatusMsg("Publish timeout: requireTimeout=" + requireTimeout);
                    log.error("Publish timeout: requireTimeout=" + requireTimeout + ", dataId=" + dataId + ",group=" + groupName
                            + ",content=" + context + ",serverId=" + serverId);
                }
                else {
                    response.setSuccess(false);
                    response.setStatusMsg("Publish fail: use getReceiveResult()");
                    log.error("Publish fail: result=" + response.getReceiveResult() + ",dataId=" + dataId + ",group=" + groupName
                            + ",content=" + context + ",serverId=" + serverId);
                }

            }
            catch (HttpException e) {
                response.setSuccess(false);
                response.setStatusMsg("Publish HttpException:" + e.getMessage());
                log.error("processPublishAfterModifiedByDefinedServerId(String dataId, String groupName, String context,String serverId)ִHttpException dataId="
                            + dataId + ",group=" + groupName + ",content=" + context + ",serverId=" + serverId, e);
                return response;
            }
            catch (IOException e) {
                response.setSuccess(false);
                response.setStatusMsg("Publish IOException:" + e.getMessage());
                log.error("processPublishAfterModifiedByDefinedServerId(String dataId, String groupName, String context,String serverId)ִIOException dataId="
                            + dataId + ",group=" + groupName + ",content=" + context + ",serverId=" + serverId, e);
                return response;
            }
            finally {
                post.releaseConnection();
            }

            return response;
        }
    }

    private boolean login(String serverId) {
        if (StringUtils.isEmpty(serverId) || StringUtils.isBlank(serverId))
            return false;
        DiamondSDKConf defaultConf = diamondSDKConfMaps.get(serverId);
        log.info("[login] serverId:" + serverId + ",config=" + defaultConf);
        if (null == defaultConf)
            return false;
        RandomDiamondUtils util = new RandomDiamondUtils();
        util.init(defaultConf.getDiamondConfs());
        if (defaultConf.getDiamondConfs().size() == 0)
            return false;
        boolean flag = false;
        log.info("[randomSequence] sequence=: " + util.getSequenceToString());
        while (util.getRetry_times() < util.getMax_times()) {

            DiamondConf diamondConf = util.generatorOneDiamondConf();
            log.info("Current retry_time=" + util.getRetry_times() + "config=" + diamondConf);
            if (diamondConf == null)
                break;
            client.getHostConfiguration().setHost(diamondConf.getDiamondIp(),
                Integer.parseInt(diamondConf.getDiamondPort()), "http");
            PostMethod post = new PostMethod("/login.do?method=login");
            post.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, requireTimeout);
            NameValuePair username_value = new NameValuePair("username", diamondConf.getDiamondUsername());
            NameValuePair password_value = new NameValuePair("password", diamondConf.getDiamondPassword());
            post.setRequestBody(new NameValuePair[] { username_value, password_value });
            log.info("diamondIp: " + diamondConf.getDiamondIp() + ",diamondPort: " + diamondConf.getDiamondPort()
                    + ",diamondUsername: " + diamondConf.getDiamondUsername() + ",diamondPassword: "
                    + diamondConf.getDiamondPassword() + "diamondServerUrl: [" + diamondConf.getDiamondConUrl() + "]");

            try {
                int state = client.executeMethod(post);
                log.info("login status=" + state);
                if (state == HttpStatus.SC_OK) {
                    log.info("Success when retry_time=" + util.getRetry_times());
                    flag = true;
                    break;
                }

            }
            catch (HttpException e) {
                log.error("login HttpException", e);
            }
            catch (IOException e) {
                log.error("login IOException", e);
            }
            finally {
                post.releaseConnection();
            }
        }
        if (!flag) {
            log.error("login fail serverId=" + serverId);
        }
        return flag;
    }

    static final String LIST_FORMAT_URL =
            "/admin.do?method=listConfig&group=%s&dataId=%s&pageNo=%d&pageSize=%d";
    static final String LIST_LIKE_FORMAT_URL =
            "/admin.do?method=listConfigLike&group=%s&dataId=%s&pageNo=%d&pageSize=%d";

    @SuppressWarnings("unchecked")
    private PageContextResult<ConfigInfo> processQuery(String dataIdPattern, String groupNamePattern,
            String contentPattern, String serverId, long currentPage, long sizeOfPerPage) {
        PageContextResult<ConfigInfo> response = new PageContextResult<ConfigInfo>();
        if (!login(serverId)) {
            response.setSuccess(false);
            response.setStatusMsg("login fail");
            return response;
        }
        if (log.isDebugEnabled())
            log.debug("processQuery(" + dataIdPattern + "," + groupNamePattern + "," + contentPattern + ","
                    + serverId + ")");
        boolean hasPattern =
                PatternUtils.hasCharPattern(dataIdPattern) || PatternUtils.hasCharPattern(groupNamePattern)
                        || PatternUtils.hasCharPattern(contentPattern);
        String url = null;
        if (hasPattern) {
            if (!StringUtils.isBlank(contentPattern)) {
                log.warn("processQuery has contentPattern, dataIdPattern=" + dataIdPattern + ",groupNamePattern=" + groupNamePattern
                        + ",contentPattern=" + contentPattern);
                url = String.format(LIST_LIKE_FORMAT_URL, groupNamePattern, dataIdPattern, 1, Integer.MAX_VALUE);
            }
            else
                url = String.format(LIST_LIKE_FORMAT_URL, groupNamePattern, dataIdPattern, currentPage, sizeOfPerPage);
        }
        else {
            url = String.format(LIST_FORMAT_URL, groupNamePattern, dataIdPattern, currentPage, sizeOfPerPage);
        }

        GetMethod method = new GetMethod(url);
        configureGetMethod(method);
        try {

            int status = client.executeMethod(method);
            response.setStatusCode(status);
            switch (status) {
            case HttpStatus.SC_OK:
                String json = "";
                try {
                    json = getContent(method).trim();

                    Page<ConfigInfo> page = null;

                    if (!json.equals("null")) {
                        page =
                                (Page<ConfigInfo>) JSONUtils.deserializeObject(json,
                                    new TypeReference<Page<ConfigInfo>>() {
                                    });
                    }
                    if (page != null) {
                        List<ConfigInfo> diamondData = page.getPageItems();
                        if (!StringUtils.isBlank(contentPattern)) {
                            Pattern pattern = Pattern.compile(contentPattern.replaceAll("\\*", ".*"));
                            List<ConfigInfo> newList = new ArrayList<ConfigInfo>();
                            Collections.sort(diamondData);
                            int totalCount = 0;
                            long begin = sizeOfPerPage * (currentPage - 1);
                            long end = sizeOfPerPage * currentPage;
                            for (ConfigInfo configInfo : diamondData) {
                                if (configInfo.getContent() != null) {
                                    Matcher m = pattern.matcher(configInfo.getContent());
                                    if (m.find()) {
                                        if (totalCount >= begin && totalCount < end) {
                                            newList.add(configInfo);
                                        }
                                        totalCount++;
                                    }
                                }
                            }
                            page.setPageItems(newList);
                            page.setTotalCount(totalCount);
                        }
                        response.setOriginalDataSize(diamondData.size());
                        response.setTotalCounts(page.getTotalCount());
                        response.setCurrentPage(currentPage);
                        response.setSizeOfPerPage(sizeOfPerPage);
                    }
                    else {
                        response.setOriginalDataSize(0);
                        response.setTotalCounts(0);
                        response.setCurrentPage(currentPage);
                        response.setSizeOfPerPage(sizeOfPerPage);
                    }
                    response.operation();
                    List<ConfigInfo> pageItems = new ArrayList<ConfigInfo>();
                    if (page != null) {
                        pageItems = page.getPageItems();
                    }
                    response.setDiamondData(pageItems);
                    response.setSuccess(true);
                    response.setStatusMsg("processQuery success.");
                    log.info("processQuery success, url=" + url);
                }
                catch (Exception e) {
                    response.setSuccess(false);
                    response.setStatusMsg("processQuery error" + e.getLocalizedMessage());
                    log.error("processQuery error, dataId=" + dataIdPattern + ",group=" + groupNamePattern + ",serverId="
                            + serverId + ",json=" + json, e);
                }
                break;
            case HttpStatus.SC_REQUEST_TIMEOUT:
                response.setSuccess(false);
                response.setStatusMsg("processQuery timeout, requireTimeout=" + requireTimeout);
                log.error("processQuery timeout: requireTimeout=" + requireTimeout + ", dataId=" + dataIdPattern + ",group="
                        + groupNamePattern + ",serverId=" + serverId);
                break;
            default:
                response.setSuccess(false);
                response.setStatusMsg("processQuery fail: status=" + status);
                log.error("processQuery fail: status=" + status + ",dataId=" + dataIdPattern + ",group=" + groupNamePattern
                        + ",serverId=" + serverId);
                break;
            }

        }
        catch (HttpException e) {
            response.setSuccess(false);
            response.setStatusMsg("processQuery http exception:" + e.getMessage());
            log.error("processQuery http exception, dataId=" + dataIdPattern + ",group=" + groupNamePattern + ",serverId=" + serverId, e);
        }
        catch (IOException e) {
            response.setSuccess(false);
            response.setStatusMsg("processQuery IOException" + e.getMessage());
            log.error("processQuery IOException, dataId=" + dataIdPattern + ",group=" + groupNamePattern + ",serverId=" + serverId, e);
        }
        finally {
            method.releaseConnection();
        }

        return response;
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

    String getContent(HttpMethod httpMethod) throws UnsupportedEncodingException {
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
            }
            catch (Exception e) {
                log.error("getContent zip exception", e);
            }
            finally {
                try {
                    br.close();
                }
                catch (Exception e1) {
                    // ignore
                }
                try {
                    isr.close();
                }
                catch (Exception e1) {
                    // ignore
                }
                try {
                    gzin.close();
                }
                catch (Exception e1) {
                    // ignore
                }
                try {
                    is.close();
                }
                catch (Exception e1) {
                    // ignore
                }
            }
        }
        else {
            String content = null;
            try {
                content = httpMethod.getResponseBodyAsString();
            }
            catch (Exception e) {
                log.error("getContent normal exception", e);
            }
            if (null == content) {
                return null;
            }
            contentBuilder.append(content);
        }
        return StringEscapeUtils.unescapeHtml(contentBuilder.toString());
    }


    private void configureGetMethod(GetMethod method) {
        method.addRequestHeader(Constants.ACCEPT_ENCODING, "gzip,deflate");
        method.addRequestHeader("Accept", "application/json");
        method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, requireTimeout);
    }

    private boolean validate(String dataId, String groupName, String context) {
        if (StringUtils.isEmpty(dataId) || StringUtils.isEmpty(groupName) || StringUtils.isEmpty(context)
                || StringUtils.isBlank(dataId) || StringUtils.isBlank(groupName) || StringUtils.isBlank(context))
            return false;
        return true;
    }


    public synchronized ContextResult unpublish(String serverId, long id) {
        return processDelete(serverId, id);
    }

    private ContextResult processDelete(String serverId, long id) {
        ContextResult response = new ContextResult();
        if (!login(serverId)) {
            response.setSuccess(false);
            response.setStatusMsg("login fail");
            return response;
        }
        log.info("processDelete(" + serverId + "," + id);
        String url = "/admin.do?method=deleteConfig&id=" + id;
        GetMethod method = new GetMethod(url);
        configureGetMethod(method);
        try {

            int status = client.executeMethod(method);
            response.setStatusCode(status);
            switch (status) {
            case HttpStatus.SC_OK:
                response.setSuccess(true);
                response.setReceiveResult(getContent(method));
                response.setStatusMsg("processDelete success, url=" + url);
                log.warn("processDelete success, url=" + url);
                break;
            case HttpStatus.SC_REQUEST_TIMEOUT:
                response.setSuccess(false);
                response.setStatusMsg("processDelete timeout: requireTimeout=" + requireTimeout);
                log.error("processDelete timeout: requireTimeout=" + requireTimeout + ", id=" + id + ",serverId=" + serverId);
                break;
            default:
                response.setSuccess(false);
                response.setStatusMsg("processDelete exception: status=" + status);
                log.error("processDelete exception: status=" + status + ", id=" + id + ",serverId=" + serverId);
                break;
            }

        }
        catch (HttpException e) {
            response.setSuccess(false);
            response.setStatusMsg("processDelete http exception" + e.getMessage());
            log.error("processDelete http exception, id=" + id + ",serverId=" + serverId, e);
        }
        catch (IOException e) {
            response.setSuccess(false);
            response.setStatusMsg("processDelete IOException" + e.getMessage());
            log.error("processDelete IOException, id=" + id + ",serverId=" + serverId, e);
        }
        finally {
            method.releaseConnection();
        }

        return response;
    }


    @Override
    public Map<String, DiamondSDKConf> getDiamondSDKConfMaps() {
        return this.diamondSDKConfMaps;
    }


    @Override
    public BatchContextResult<ConfigInfoEx> batchQuery(String serverId, String groupName, List<String> dataIds) {
        BatchContextResult<ConfigInfoEx> response = new BatchContextResult<ConfigInfoEx>();

        if (dataIds == null) {
            log.error("dataId list cannot be null, serverId=" + serverId + ",group=" + groupName);
            response.setSuccess(false);
            response.setStatusMsg("dataId list cannot be null");

            return response;
        }

        StringBuilder dataIdBuilder = new StringBuilder();
        for (String dataId : dataIds) {
            dataIdBuilder.append(dataId).append(Constants.LINE_SEPARATOR);
        }
        String dataIdStr = dataIdBuilder.toString();
        if (!login(serverId)) {
            response.setSuccess(false);
            response.setStatusMsg("login fail, serverId=" + serverId);
            return response;
        }

        PostMethod post = new PostMethod("/admin.do?method=batchQuery");
        post.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, requireTimeout);
        try {
            NameValuePair dataId_value = new NameValuePair("dataIds", dataIdStr);
            NameValuePair group_value = new NameValuePair("group", groupName);

            post.setRequestBody(new NameValuePair[] { dataId_value, group_value });

            int status = client.executeMethod(post);
            response.setStatusCode(status);
            String responseMsg = post.getResponseBodyAsString();
            response.setResponseMsg(responseMsg);

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
                    response.getResult().addAll(configInfoExList);

                    response.setSuccess(true);
                    response.setStatusMsg("batch query success");
                    log.info("batch query success, serverId=" + serverId + ",dataIds=" + dataIdStr + ",group="
                            + groupName + ",json=" + json);
                }
                catch (Exception e) {
                    response.setSuccess(false);
                    response.setStatusMsg("batch query deserialize error");
                    log.error("batch query deserialize error, serverId=" + serverId + ",dataIdStr=" + dataIdStr
                            + ",group=" + groupName + ",json=" + json, e);
                }

            }
            else if (status == HttpStatus.SC_REQUEST_TIMEOUT) {
                response.setSuccess(false);
                response.setStatusMsg("batch query timeout, socket timeout(ms):" + requireTimeout);
                log.error("batch query timeout, socket timeout(ms):" + requireTimeout + ", serverId=" + serverId
                        + ",dataIds=" + dataIdStr + ",group=" + groupName);
            }
            else {
                response.setSuccess(false);
                response.setStatusMsg("batch query fail, status:" + status);
                log.error("batch query fail, status:" + status + ", response:" + responseMsg + ",serverId=" + serverId
                        + ",dataIds=" + dataIdStr + ",group=" + groupName);
            }
        }
        catch (HttpException e) {
            response.setSuccess(false);
            response.setStatusMsg("batch query http exception��" + e.getMessage());
            log.error("batch query http exception, serverId=" + serverId + ",dataIds=" + dataIdStr + ",group="
                    + groupName, e);
        }
        catch (IOException e) {
            response.setSuccess(false);
            response.setStatusMsg("batch query io exception��" + e.getMessage());
            log.error("batch query io exception, serverId=" + serverId + ",dataIds=" + dataIdStr + ",group="
                    + groupName, e);
        }
        finally {
            post.releaseConnection();
        }

        return response;
    }


    @Override
    public BatchContextResult<ConfigInfoEx> batchAddOrUpdate(String serverId, String groupName,
            Map<String, String> dataId2ContentMap) {
        BatchContextResult<ConfigInfoEx> response = new BatchContextResult<ConfigInfoEx>();

        if (dataId2ContentMap == null) {
            log.error("dataId2ContentMap cannot be null, serverId=" + serverId + " ,group=" + groupName);
            response.setSuccess(false);
            response.setStatusMsg("dataId2ContentMap cannot be null");
            return response;
        }

        StringBuilder allDataIdAndContentBuilder = new StringBuilder();
        for (String dataId : dataId2ContentMap.keySet()) {
            String content = dataId2ContentMap.get(dataId);
            allDataIdAndContentBuilder.append(dataId + Constants.WORD_SEPARATOR + content).append(
                Constants.LINE_SEPARATOR);
        }
        String allDataIdAndContent = allDataIdAndContentBuilder.toString();

        if (!login(serverId)) {
            response.setSuccess(false);
            response.setStatusMsg("login fail, serverId=" + serverId);
            return response;
        }

        PostMethod post = new PostMethod("/admin.do?method=batchAddOrUpdate");
        post.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, requireTimeout);
        try {
            NameValuePair dataId_value = new NameValuePair("allDataIdAndContent", allDataIdAndContent);
            NameValuePair group_value = new NameValuePair("group", groupName);

            post.setRequestBody(new NameValuePair[] { dataId_value, group_value });

            int status = client.executeMethod(post);
            response.setStatusCode(status);
            String responseMsg = post.getResponseBodyAsString();
            response.setResponseMsg(responseMsg);

            if (status == HttpStatus.SC_OK) {
                String json = null;
                try {
                    json = responseMsg;

                    List<ConfigInfoEx> configInfoExList = new LinkedList<ConfigInfoEx>();
                    Object resultObj = JSONUtils.deserializeObject(json, new TypeReference<List<ConfigInfoEx>>() {
                    });
                    if (!(resultObj instanceof List<?>)) {
                        throw new RuntimeException("batch write deserialize type error, not list, json=" + json);
                    }
                    List<ConfigInfoEx> resultList = (List<ConfigInfoEx>) resultObj;
                    for (ConfigInfoEx configInfoEx : resultList) {
                        configInfoExList.add(configInfoEx);
                    }
                    response.getResult().addAll(configInfoExList);
                    response.setStatusMsg("batch write success");
                    log.info("batch write success,serverId=" + serverId + ",allDataIdAndContent=" + allDataIdAndContent
                            + ",group=" + groupName + ",json=" + json);
                }
                catch (Exception e) {
                    response.setSuccess(false);
                    response.setStatusMsg("batch write deserialize error");
                    log.error("batch write deserialize error, serverId=" + serverId + ",allDataIdAndContent="
                            + allDataIdAndContent + ",group=" + groupName + ",json=" + json, e);
                }
            }
            else if (status == HttpStatus.SC_REQUEST_TIMEOUT) {
                response.setSuccess(false);
                response.setStatusMsg("batch write timeout, socket timeout(ms):" + requireTimeout);
                log.error("batch write timeout, socket timeout(ms):" + requireTimeout + ", serverId=" + serverId
                        + ",allDataIdAndContent=" + allDataIdAndContent + ",group=" + groupName);
            }
            else {
                response.setSuccess(false);
                response.setStatusMsg("batch write fail, status:" + status);
                log.error("batch write fail, status:" + status + ", response:" + responseMsg + ",serverId=" + serverId
                        + ",allDataIdAndContent=" + allDataIdAndContent + ",group=" + groupName);
            }
        }
        catch (HttpException e) {
            response.setSuccess(false);
            response.setStatusMsg("batch write http exception��" + e.getMessage());
            log.error("batch write http exception, serverId=" + serverId + ",allDataIdAndContent="
                    + allDataIdAndContent + ",group=" + groupName, e);
        }
        catch (IOException e) {
            response.setSuccess(false);
            response.setStatusMsg("batch write io exception��" + e.getMessage());
            log.error("batch write io exception, serverId=" + serverId + ",allDataIdAndContent=" + allDataIdAndContent
                    + ",group=" + groupName, e);
        }
        finally {
            post.releaseConnection();
        }

        return response;
    }

    public DiamondSDKManagerImpl setDiamondSDKConfMaps(Map<String, DiamondSDKConf> diamondSDKConfMaps) {
        this.diamondSDKConfMaps = diamondSDKConfMaps;
        return this;
    }
}
