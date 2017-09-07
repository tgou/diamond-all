/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.server.controller;

import com.taobao.diamond.common.Constants;
import com.taobao.diamond.domain.ConfigInfo;
import com.taobao.diamond.domain.ConfigInfoEx;
import com.taobao.diamond.server.service.ConfigService;
import com.taobao.diamond.server.service.DiskService;
import com.taobao.diamond.server.utils.GlobalCounter;
import com.taobao.diamond.utils.JSONUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.taobao.diamond.common.Constants.LINE_SEPARATOR;
import static com.taobao.diamond.common.Constants.WORD_SEPARATOR;


/**
 * @author boyan
 * @date 2010-5-4
 */
@Controller
public class ConfigController {

    private static final Log log = LogFactory.getLog(ConfigController.class);

    @Autowired
    private ConfigService configService;

    @Autowired
    private DiskService diskService;

    public String getConfig(HttpServletRequest request, HttpServletResponse response, String dataId, String group) {
        response.setHeader("Content-Type", "text/html;charset=UTF-8");
        final String address = getRemortIP(request);
        if (address == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return "400";
        }

        if (GlobalCounter.getCounter().decrementAndGet() >= 0) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return "503";
        }

        String md5 = this.configService.getContentMD5(dataId, group);
        if (md5 == null) {
            return "404";
        }

        response.setHeader(Constants.CONTENT_MD5, md5);

        if (diskService.isModified(dataId, group)) {
            response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return "304";
        }
        String path = configService.getConfigInfoPath(dataId, group);
        if (diskService.isModified(dataId, group)) {
            response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return "304";
        }
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
        response.setHeader("Cache-Control", "no-cache,no-store");
        return "forward:" + path;
    }


    public String getProbeModifyResult(HttpServletRequest request, HttpServletResponse response, String probeModify) {
        response.setHeader("Content-Type", "text/html;charset=UTF-8");
        final String address = getRemortIP(request);
        if (address == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return "400";
        }

        if (GlobalCounter.getCounter().decrementAndGet() >= 0) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return "503";
        }

        final List<ConfigKey> configKeyList = getConfigKeyList(probeModify);

        StringBuilder resultBuilder = new StringBuilder();
        for (ConfigKey key : configKeyList) {
            String md5 = this.configService.getContentMD5(key.getDataId(), key.getGroup());
            if (!StringUtils.equals(md5, key.getMd5())) {
                resultBuilder.append(key.getDataId()).append(WORD_SEPARATOR).append(key.getGroup())
                        .append(LINE_SEPARATOR);
            }
        }

        String returnHeader = resultBuilder.toString();
        try {
            returnHeader = URLEncoder.encode(resultBuilder.toString(), "UTF-8");
        } catch (Exception e) {
            // ignore
        }

        request.setAttribute("content", returnHeader);
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
        response.setHeader("Cache-Control", "no-cache,no-store");
        return "200";
    }

    public String batchQuery(HttpServletRequest request, HttpServletResponse response) { // TODO: performance tuning
        String group = request.getParameter("group");
        String dataIds = request.getParameter("dataIds");

        response.setCharacterEncoding("UTF-8");

        // 这里抛出的异常, 会产生一个500错误, 返回给sdk, sdk会将500错误记录到日志中
        if (StringUtils.isBlank(dataIds)) {
            throw new IllegalArgumentException("批量查询, dataIds不能为空");
        }
        // group对批量操作的每一条数据都相同, 不需要在for循环里面进行判断
        if (StringUtils.isBlank(group)) {
            throw new IllegalArgumentException("批量查询, group不能为空或者包含非法字符");
        }

        // 分解dataId
        String[] dataIdArray = dataIds.split(Constants.LINE_SEPARATOR);
        group = group.trim();

        List<ConfigInfoEx> configInfoExList = new ArrayList<ConfigInfoEx>();
        for (String dataId : dataIdArray) {
            ConfigInfoEx configInfoEx = new ConfigInfoEx();
            configInfoEx.setDataId(dataId);
            configInfoEx.setGroup(group);
            configInfoExList.add(configInfoEx);
            try {
                if (StringUtils.isBlank(dataId)) {
                    configInfoEx.setStatus(Constants.BATCH_QUERY_NONEXISTS);
                    configInfoEx.setMessage("dataId is blank");
                    continue;
                }

                // 查询数据库
                ConfigInfo configInfo = this.configService.findConfigInfo(dataId, group);
                if (configInfo == null) {
                    // 没有异常, 说明查询成功, 但数据不存在, 设置不存在的状态码
                    configInfoEx.setStatus(Constants.BATCH_QUERY_NONEXISTS);
                    configInfoEx.setMessage("query data does not exist");
                } else {
                    // 没有异常, 说明查询成功, 而且数据存在, 设置存在的状态码
                    String content = configInfo.getContent();
                    configInfoEx.setContent(content);
                    configInfoEx.setStatus(Constants.BATCH_QUERY_EXISTS);
                    configInfoEx.setMessage("query success");
                }
            } catch (Exception e) {
                log.error("批量查询, 在查询这个dataId时出错, dataId=" + dataId + ",group=" + group, e);
                // 出现异常, 设置异常状态码
                configInfoEx.setStatus(Constants.BATCH_OP_ERROR);
                configInfoEx.setMessage("query error: " + e.getMessage());
            }
        }

        String json = null;
        try {
            json = JSONUtils.serializeObject(configInfoExList);
        } catch (Exception e) {
            log.error("批量查询结果序列化出错, json=" + json, e);
        }

        return json;
    }

    public ConfigService getConfigService() {
        return configService;
    }


    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }


    public DiskService getDiskService() {
        return diskService;
    }


    public void setDiskService(DiskService diskService) {
        this.diskService = diskService;
    }


    /**
     * @param request
     * @return
     */
    public String getRemortIP(HttpServletRequest request) {
        if (request.getHeader("x-forwarded-for") == null) {
            return request.getRemoteAddr();
        }
        return request.getHeader("x-forwarded-for");
    }


    public static List<ConfigKey> getConfigKeyList(String configKeysString) {
        List<ConfigKey> configKeyList = new LinkedList<ConfigKey>();
        if (null == configKeysString || "".equals(configKeysString)) {
            return configKeyList;
        }
        String[] configKeyStrings = configKeysString.split(LINE_SEPARATOR);
        for (String configKeyString : configKeyStrings) {
            String[] configKey = configKeyString.split(WORD_SEPARATOR);
            if (configKey.length > 3) {
                continue;
            }
            ConfigKey key = new ConfigKey();
            if ("".equals(configKey[0])) {
                continue;
            }
            key.setDataId(configKey[0]);
            if (configKey.length >= 2 && !"".equals(configKey[1])) {
                key.setGroup(configKey[1]);
            }
            if (configKey.length == 3 && !"".equals(configKey[2])) {
                key.setMd5(configKey[2]);
            }
            configKeyList.add(key);
        }

        return configKeyList;
    }

    public static class ConfigKey {
        private String dataId;
        private String group;
        private String md5;


        public String getDataId() {
            return dataId;
        }


        public void setDataId(String dataId) {
            this.dataId = dataId;
        }


        public String getGroup() {
            return group;
        }


        public void setGroup(String group) {
            this.group = group;
        }


        public String getMd5() {
            return md5;
        }


        public void setMd5(String md5) {
            this.md5 = md5;
        }


        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("DataID: ").append(dataId).append("\r\n");
            sb.append("Group: ").append((null == group ? "" : group)).append("\r\n");
            sb.append("MD5: ").append((null == md5 ? "" : md5)).append("\r\n");
            return sb.toString();
        }
    }
}
