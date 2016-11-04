package com.taobao.diamond.client;

import com.taobao.diamond.client.impl.DiamondEnv;
import com.taobao.diamond.client.impl.DiamondEnvRepo;
import com.taobao.diamond.domain.ConfigInfoEx;
import com.taobao.diamond.manager.ManagerListener;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Created by diwayou on 2015/11/25.
 */
public class DiamondEnvTest {

    private DiamondEnv diamondEnv;

    @Before
    public void before() {
        diamondEnv = DiamondEnvRepo.defaultEnv;
    }

    @Test
    public void batchQueryTest() {
        BatchHttpResult result = diamondEnv.batchQuery(Arrays.asList("OFFLINE_URLS", "MEMBER_URLS"), "API_URL_GROUP", 10000);

        if (result.isSuccess()) {
            List<ConfigInfoEx> configInfoExList = result.getResult();
            for (ConfigInfoEx configInfoEx : configInfoExList) {
                System.out.println(configInfoEx);
            }
        } else {
            System.out.println(result.getStatusCode());
        }
    }

    @Test
    public void addListenerTest() throws InterruptedException {
        ManagerListener managerListener = new ManagerListener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println(configInfo);
            }
        };

        diamondEnv.addListeners("diwayou", "TEST", managerListener);

        TimeUnit.HOURS.sleep(1);
    }
}
