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

import com.taobao.diamond.configinfo.CacheData;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author aoqiong
 */
public interface DiamondSubscriber extends DiamondClientSub {
    SubscriberListener getSubscriberListener();

    /**
     * set async subscriber listener, can dynamic replace
     *
     * @param subscriberListener
     */
    void setSubscriberListener(SubscriberListener subscriberListener);

    /**
     * ${user.home}/diamond/data => diamond server
     *
     * @param dataId
     * @param group
     * @param timeout
     * @return
     */
    String getConfigureInformation(String dataId, String group, long timeout);


    /**
     * ${user.home}/diamond/data => diamond server
     *
     * @param dataId
     * @param timeout
     * @return
     */
    String getConfigureInformation(String dataId, long timeout);


    /**
     * local file => diamond server => snapshot, if not exist return null.
     *
     * @param dataId
     * @param group
     * @param timeout
     * @return
     */
    String getAvailableConfigureInformation(String dataId, String group, long timeout);

    /**
     * local file => snapshot, if not exist return null
     *
     * @param dataId
     * @param group
     * @param timeout
     * @return
     */
    String getFromLocalAndSnapshot(String dataId, String group, long timeout);


    /**
     * add dataId and group, if exist, replace
     *
     * @param dataId
     * @param group
     */
    void addDataId(String dataId, String group);


    /**
     * add dataId, use DEFAULT_GROUP, if exist , replace
     *
     * @param dataId
     */
    void addDataId(String dataId);


    /**
     * if contain ConfigInfo for dataId and DEFAULT_GROUP
     *
     * @param dataId
     * @return
     */
    boolean containDataId(String dataId);


    /**
     * if contain ConfigInfo for dataId and group
     *
     * @param dataId
     * @param group
     * @return
     */
    boolean containDataId(String dataId, String group);

    void removeDataId(String dataId);

    void removeDataId(String dataId, String group);

    void clearAllDataIds();

    Set<String> getDataIds();

    ConcurrentHashMap<String, ConcurrentHashMap<String, CacheData>> getCache();


    /**
     * snapshot => local file => diamond server
     *
     * @param dataId
     * @param group
     * @param timeout
     * @return
     */
    String getAvailableConfigureInformationFromSnapshot(String dataId, String group, long timeout);

    /**
     * batch query from diamond server
     *
     * @param dataIds
     * @param group
     * @param timeout
     * @return
     */
    BatchHttpResult getConfigureInformationBatch(List<String> dataIds, String group, int timeout);
}
