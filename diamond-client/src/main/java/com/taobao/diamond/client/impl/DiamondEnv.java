package com.taobao.diamond.client.impl;

import com.taobao.diamond.client.BatchHttpResult;
import com.taobao.diamond.client.DiamondConfigureUtil;
import com.taobao.diamond.client.DiamondSubscriber;
import com.taobao.diamond.common.Constants;
import com.taobao.diamond.domain.ConfigInfoEx;
import com.taobao.diamond.manager.ManagerListener;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by diwayou on 2015/11/18.
 */
public class DiamondEnv {

    private static final Log log = LogFactory.getLog(DiamondEnv.class);

    private DiamondSubscriber diamondSubscriber;

    public DiamondEnv() {
        diamondSubscriber = DiamondClientFactory.getSingletonDiamondSubscriber();

        diamondSubscriber.setDiamondConfigure(DiamondConfigureUtil.getFromEnv());

        diamondSubscriber.start();
    }

    /**
     * 获取配置信息
     * @param dataId
     * @param group
     * @param fromWhere See Constants
     * @param timeout
     * @return
     * @throws IOException
     */
    public String getConfig(String dataId, String group, int fromWhere, long timeout) throws IOException {
        if (group == null) {
            group = Constants.DEFAULT_GROUP;
        }

        switch (fromWhere) {
            case Constants.GETCONFIG_LOCAL_SERVER_SNAPSHOT:
                return diamondSubscriber.getAvailableConfigureInformation(dataId, group, timeout);
            case Constants.GETCONFIG_SNAPSHOT_LOCAL_SERVER:
                return diamondSubscriber.getAvailableConfigureInformationFromSnapshot(dataId, group, timeout);
            default:
                throw new IllegalArgumentException("FromWhere is illegal.");
        }
    }

    public BatchHttpResult batchQuery(List<String> dataIds, String group, int timeout) {
        BatchHttpResult result = batchQueryFromLocal(dataIds, group, timeout);
        if (result.isSuccess()) {
            return result;
        }

        return diamondSubscriber.getConfigureInformationBatch(dataIds, group, timeout);
    }

    private BatchHttpResult batchQueryFromLocal(List<String> dataIds, String group, int timeout) {
        List<ConfigInfoEx> configInfoExList = new ArrayList<ConfigInfoEx>(dataIds.size());

        for (String dataId : dataIds) {
            ConfigInfoEx configInfoEx = new ConfigInfoEx(dataId, group, null);
            String content = null;
            try {
                content = diamondSubscriber.getFromLocalAndSnapshot(dataId, group, timeout);
            } catch (Exception e) {
                log.warn("batchQuery error: dataIds=" + dataIds.toString() + " group=" + group, e);
            }

            if (StringUtils.isBlank(content)) {
                log.info("batchQueryFromLocal content is blank. dataId=" + dataId + " group=" + group);

                return new BatchHttpResult(Constants.BATCH_OP_ERROR);
            }

            configInfoEx.setContent(content);
            configInfoEx.setStatus(Constants.BATCH_QUERY_EXISTS);

            configInfoExList.add(configInfoEx);
        }

        return new BatchHttpResult(configInfoExList);
    }

    public List<ManagerListener> getListeners(String dataId, String group) {
        if (group == null) {
            group = Constants.DEFAULT_GROUP;
        }

        return ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener())
                .getManagerListenerList(dataId, group);
    }

    public void addListeners(String dataId, String group, ManagerListener... managerListeners) {
        if (group == null) {
            group = Constants.DEFAULT_GROUP;
        }
        for (ManagerListener managerListener : managerListeners) {
            ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener())
                    .addManagerListener(dataId, group, managerListener);
        }
    }

    public void removeListener(String dataId, String group) {
        if (group == null) {
            group = Constants.DEFAULT_GROUP;
        }

        ((DefaultSubscriberListener) diamondSubscriber.getSubscriberListener())
                .removeManagerListeners(dataId, group);
    }
}
