package com.taobao.diamond.manager;

import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Created by diwayou on 2015/11/20.
 */
public class DefaultDiamondManagerTest {

    private DefaultDiamondManager diamondManager;

    @Before
    public void before() {
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
        diamondManager = new DefaultDiamondManager("DEFAULT_GROUP", "a", Arrays.asList(managerListener));
    }

    @Test
    public void bootstrap() throws InterruptedException {
        TimeUnit.HOURS.sleep(1);
    }
}
