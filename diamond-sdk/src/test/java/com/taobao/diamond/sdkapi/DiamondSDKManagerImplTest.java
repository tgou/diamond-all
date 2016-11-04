package com.taobao.diamond.sdkapi;

import com.taobao.diamond.domain.*;
import com.taobao.diamond.sdkapi.impl.DiamondSDKManagerImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by diwayou on 2016/11/3.
 */
public class DiamondSDKManagerImplTest {

    private DiamondSDKManagerImpl diamondSDKManager;

    @Before
    public void before() {
        diamondSDKManager = new DiamondSDKManagerImpl();

        Map<String, DiamondSDKConf> diamondSDKConfMap = new HashMap<String, DiamondSDKConf>();
        DiamondSDKConf diamondSDKConf = new DiamondSDKConf(Arrays.asList(new DiamondConf("127.0.0.1", "8080", "abc", "123")));
        diamondSDKConfMap.put("1", diamondSDKConf);

        diamondSDKManager.setDiamondSDKConfMaps(diamondSDKConfMap);
    }

    @Test
    public void publishTest() {
        ContextResult result = diamondSDKManager.publish("DATA_ID_TEST", "TEST_GROUP", "1=2", "1");

        System.out.println(result);
    }

    @Test
    public void publishAfterModifiedTest() {
        ContextResult result = diamondSDKManager.publishAfterModified("DATA_ID_TEST", "TEST_GROUP", "1=2", "1");

        System.out.println(result);
    }

    @Test
    public void queryByTest() {
        PageContextResult<ConfigInfo> result = diamondSDKManager.queryBy("*TEST*", "*TEST_GROUP*", "1", 1, 100);
        System.out.println(result);

        List<ConfigInfo> configInfoList = result.getDiamondData();
        if (configInfoList != null) {
            for (ConfigInfo configInfo : configInfoList) {
                System.out.println(configInfo);
            }
        }
    }

    @Test
    public void queryByMoreTest() {
        PageContextResult<ConfigInfo> result = diamondSDKManager.queryBy("*TEST*", "*TEST_GROUP*", "*1*", "1", 1, 100);
        System.out.println(result);

        List<ConfigInfo> configInfoList = result.getDiamondData();
        if (configInfoList != null) {
            for (ConfigInfo configInfo : configInfoList) {
                System.out.println(configInfo);
                System.out.println(configInfo.getId());
            }
        }
    }

    @Test
    public void queryByDataIdAndGroupNameTest() {
        ContextResult result = diamondSDKManager.queryByDataIdAndGroupName("DATA_ID_TEST", "TEST_GROUP", "1");

        System.out.println(result);
    }

    @Test
    public void unpublishTest() {
        ContextResult result = diamondSDKManager.unpublish("1", 2);

        System.out.println(result);
    }

    @Test
    public void batchQueryTest() {
        BatchContextResult<ConfigInfoEx> result = diamondSDKManager.batchQuery("1", "TEST_GROUP", Arrays.asList("DATA_ID_TEST", "DATA_ID_TEST"));

        System.out.println(result);
    }

    @Test
    public void batchAddOrUpdateTest() {
        Map<String, String> dataId2Content = new HashMap<String, String>();
        dataId2Content.put("DATA_ID_1", "1=1");
        dataId2Content.put("DATA_ID_2", "2=2");
        dataId2Content.put("DATA_ID_3", "3=3");
        BatchContextResult<ConfigInfoEx> result = diamondSDKManager.batchAddOrUpdate("1", "TEST_GROUP", dataId2Content);

        System.out.println(result);
    }
}
