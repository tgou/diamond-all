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

import com.taobao.diamond.common.Constants;
import com.taobao.diamond.domain.ConfigInfo;
import com.taobao.diamond.domain.Page;
import com.taobao.diamond.md5.MD5;
import com.taobao.diamond.server.exception.ConfigServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.concurrent.ConcurrentHashMap;


@Service
public class ConfigService {

    private static final Log log = LogFactory.getLog(ConfigService.class);

    @Autowired
    private PersistService persistService;

    @Autowired
    private DiskService diskService;

    @Autowired
    private NotifyService notifyService;

    private final ConcurrentHashMap<String, String> contentMD5Cache = new ConcurrentHashMap<String, String>();


    public String getConfigInfoPath(String dataId, String group) {
        StringBuilder sb = new StringBuilder("/");
        sb.append(Constants.BASE_DIR).append("/");
        sb.append(group).append("/");
        sb.append(dataId);
        return sb.toString();
    }


    public void updateMD5Cache(ConfigInfo configInfo) {
        this.contentMD5Cache.put(generateMD5CacheKey(configInfo.getDataId(), configInfo.getGroup()), MD5.getInstance()
                .getMD5String(configInfo.getContent()));
    }


    public String getContentMD5(String dataId, String group) {
        String key = generateMD5CacheKey(dataId, group);
        String md5 = this.contentMD5Cache.get(key);
        if (md5 == null) {
            synchronized (this) {
                return this.contentMD5Cache.get(key);
            }
        } else {
            return md5;
        }
    }


    String generateMD5CacheKey(String dataId, String group) {
        String key = group + "/" + dataId;
        return key;
    }


    String generatePath(String dataId, final String group) {
        StringBuilder sb = new StringBuilder("/");
        sb.append(Constants.BASE_DIR).append("/");
        sb.append(group).append("/");
        sb.append(dataId);
        return sb.toString();
    }


    public void removeConfigInfo(long id) {
        try {
            ConfigInfo configInfo = this.persistService.findConfigInfo(id);
            this.diskService.removeConfigInfo(configInfo.getDataId(), configInfo.getGroup());
            this.contentMD5Cache.remove(generateMD5CacheKey(configInfo.getDataId(), configInfo.getGroup()));
            this.persistService.removeConfigInfo(configInfo);
            this.notifyOtherNodes(configInfo.getDataId(), configInfo.getGroup());

        } catch (Exception e) {
            log.error("Delete config error", e);
            throw new ConfigServiceException(e);
        }
    }


    public void addConfigInfo(String dataId, String group, String content) {
        checkParameter(dataId, group, content);
        ConfigInfo configInfo = new ConfigInfo(dataId, group, content);
        try {
            persistService.addConfigInfo(configInfo);
            this.contentMD5Cache.put(generateMD5CacheKey(dataId, group), configInfo.getMd5());
            diskService.saveToDisk(configInfo);
            this.notifyOtherNodes(dataId, group);
        } catch (Exception e) {
            log.error("Save ConfigInfo error", e);
            throw new ConfigServiceException(e);
        }
    }

    public void updateConfigInfo(String dataId, String group, String content) {
        checkParameter(dataId, group, content);
        ConfigInfo configInfo = new ConfigInfo(dataId, group, content);
        try {
            persistService.updateConfigInfo(configInfo);
            this.contentMD5Cache.put(generateMD5CacheKey(dataId, group), configInfo.getMd5());
            diskService.saveToDisk(configInfo);
            this.notifyOtherNodes(dataId, group);
        } catch (Exception e) {
            log.error("Save ConfigInfo fail", e);
            throw new ConfigServiceException(e);
        }
    }

    public void loadConfigInfoToDisk(String dataId, String group) {
        try {
            ConfigInfo configInfo = this.persistService.findConfigInfo(dataId, group);
            if (configInfo != null) {
                this.contentMD5Cache.put(generateMD5CacheKey(dataId, group), configInfo.getMd5());
                this.diskService.saveToDisk(configInfo);
            } else {
                this.contentMD5Cache.remove(generateMD5CacheKey(dataId, group));
                this.diskService.removeConfigInfo(dataId, group);
            }
        } catch (Exception e) {
            log.error("Save ConfigInfo to disk fail", e);
            throw new ConfigServiceException(e);
        }
    }


    public ConfigInfo findConfigInfo(String dataId, String group) {
        return persistService.findConfigInfo(dataId, group);
    }

    public Page<ConfigInfo> findConfigInfo(final int pageNo, final int pageSize, final String group, final String dataId) {
        if (StringUtils.hasLength(dataId) && StringUtils.hasLength(group)) {
            ConfigInfo ConfigInfo = this.persistService.findConfigInfo(dataId, group);
            Page<ConfigInfo> page = new Page<ConfigInfo>();
            if (ConfigInfo != null) {
                page.setPageNumber(1);
                page.setTotalCount(1);
                page.setPagesAvailable(1);
                page.getPageItems().add(ConfigInfo);
            }
            return page;
        } else if (StringUtils.hasLength(dataId) && !StringUtils.hasLength(group)) {
            return this.persistService.findConfigInfoByDataId(pageNo, pageSize, dataId);
        } else if (!StringUtils.hasLength(dataId) && StringUtils.hasLength(group)) {
            return this.persistService.findConfigInfoByGroup(pageNo, pageSize, group);
        } else {
            return this.persistService.findAllConfigInfo(pageNo, pageSize);
        }
    }

    public Page<ConfigInfo> findConfigInfoLike(final int pageNo, final int pageSize, final String group,
                                               final String dataId) {
        return this.persistService.findConfigInfoLike(pageNo, pageSize, dataId, group);
    }


    private void checkParameter(String dataId, String group, String content) {
        if (!StringUtils.hasLength(dataId) || StringUtils.containsWhitespace(dataId))
            throw new ConfigServiceException("Illegal dataId");

        if (!StringUtils.hasLength(group) || StringUtils.containsWhitespace(group))
            throw new ConfigServiceException("Illegal group");

        if (!StringUtils.hasLength(content))
            throw new ConfigServiceException("Illegal content");
    }


    private void notifyOtherNodes(String dataId, String group) {
        this.notifyService.notifyConfigInfoChange(dataId, group);
    }


    public DiskService getDiskService() {
        return diskService;
    }


    public void setDiskService(DiskService diskService) {
        this.diskService = diskService;
    }


    public PersistService getPersistService() {
        return persistService;
    }


    public void setPersistService(PersistService persistService) {
        this.persistService = persistService;
    }


    public NotifyService getNotifyService() {
        return notifyService;
    }


    public void setNotifyService(NotifyService notifyService) {
        this.notifyService = notifyService;
    }

}
