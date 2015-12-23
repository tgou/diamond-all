package com.taobao.diamond.server.service;

import com.taobao.diamond.domain.ConfigInfo;
import com.taobao.diamond.domain.Page;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;


/**
 * @author boyan
 * @date 2010-5-10
 */
public final class DumpConfigInfoTask implements Runnable {

    private static final Log log = LogFactory.getLog(DumpConfigInfoTask.class);

    private static final int PAGE_SIZE = 1000;

    private final TimerTaskService timerTaskService;


    public DumpConfigInfoTask(TimerTaskService timerTaskService) {
        this.timerTaskService = timerTaskService;
    }


    public void run() {
        try {
            Page<ConfigInfo> page = this.timerTaskService.getPersistService().findAllConfigInfo(1, PAGE_SIZE);
            if (page != null) {
                int totalPages = page.getPagesAvailable();
                updateConfigInfo(page);
                if (totalPages > 1) {
                    for (int pageNo = 2; pageNo <= totalPages; pageNo++) {
                        page = this.timerTaskService.getPersistService().findAllConfigInfo(pageNo, PAGE_SIZE);
                        if (page != null) {
                            updateConfigInfo(page);
                        }
                    }
                }
            }
        }
        catch (Throwable t) {
            log.error("dump task run error", t);
        }
    }


    private void updateConfigInfo(Page<ConfigInfo> page) throws IOException {
        for (ConfigInfo configInfo : page.getPageItems()) {
            if (configInfo == null) {
                continue;
            }
            try {
                this.timerTaskService.getConfigService().updateMD5Cache(configInfo);
                this.timerTaskService.getDiskService().saveToDisk(configInfo);
            }
            catch (Throwable t) {
                log.error(
                    "dump config info error, dataId=" + configInfo.getDataId() + ", group=" + configInfo.getGroup(), t);
            }

        }
    }

}