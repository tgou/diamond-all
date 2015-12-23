/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.client.processor;

import com.taobao.diamond.common.Constants;
import com.taobao.diamond.configinfo.CacheData;
import com.taobao.diamond.io.FileSystem;
import com.taobao.diamond.io.Path;
import com.taobao.diamond.io.watch.StandardWatchEventKind;
import com.taobao.diamond.io.watch.WatchEvent;
import com.taobao.diamond.io.watch.WatchKey;
import com.taobao.diamond.io.watch.WatchService;
import com.taobao.diamond.utils.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class LocalConfigInfoProcessor {
    private static final Log log = LogFactory.getLog(LocalConfigInfoProcessor.class);
    private ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor();;

    private final Map<String/* filePath */, Long/* version */> existFiles = new HashMap<String, Long>();

    private volatile boolean isRun;
    private String rootPath = null;

    /**
     * Get config from local
     * @param cacheData
     * @param force if true, do not return null when config not change
     * @return
     * @throws IOException
     */
    public String getLocalConfigureInfomation(CacheData cacheData, boolean force) throws IOException {
        String filePath = getFilePath(cacheData.getDataId(), cacheData.getGroup());
        if (!existFiles.containsKey(filePath)) {
            if (cacheData.isUseLocalConfigInfo()) {
                cacheData.setLastModifiedHeader(Constants.NULL);
                cacheData.setMd5(Constants.NULL);
                cacheData.setLocalConfigInfoFile(null);
                cacheData.setLocalConfigInfoVersion(0L);
                cacheData.setUseLocalConfigInfo(false);
            }
            return null;
        }
        if (force) {
            log.info("Get from local, dataId:" + cacheData.getDataId() + ", group:" + cacheData.getGroup());

            String content = FileUtils.getFileContent(filePath);
            return content;
        }
        if (!filePath.equals(cacheData.getLocalConfigInfoFile())
                || existFiles.get(filePath) != cacheData.getLocalConfigInfoVersion()) {
            String content = FileUtils.getFileContent(filePath);
            cacheData.setLocalConfigInfoFile(filePath);
            cacheData.setLocalConfigInfoVersion(existFiles.get(filePath));
            cacheData.setUseLocalConfigInfo(true);

            if (log.isInfoEnabled()) {
                log.info("Local config changed, dataId:" + cacheData.getDataId() + ", group:" + cacheData.getGroup());
            }

            return content;
        }
        else {
            cacheData.setUseLocalConfigInfo(true);

            if (log.isInfoEnabled()) {
                log.debug("Local config not changed, dataId:" + cacheData.getDataId() + ", group:" + cacheData.getGroup());
            }

            return null;
        }
    }


    String getFilePath(String dataId, String group) {
        StringBuilder filePathBuilder = new StringBuilder();
        filePathBuilder.append(rootPath).append("/").append(Constants.BASE_DIR).append("/").append(group).append("/")
            .append(dataId);
        File file = new File(filePathBuilder.toString());
        return file.getAbsolutePath();
    }


    public synchronized void start(String rootPath) {
        if (this.isRun) {
            return;
        }
        this.rootPath = rootPath;
        this.isRun = true;
        if (this.singleExecutor == null || singleExecutor.isTerminated()) {
            singleExecutor = Executors.newSingleThreadScheduledExecutor();
        }
        initDataDir(rootPath);
        startCheckLocalDir(rootPath);
    }


    private void initDataDir(String rootPath) {
        try {
            File flie = new File(rootPath);
            flie.mkdir();
        }
        catch (Exception e) {
        }
    }


    public synchronized void stop() {
        if (!this.isRun) {
            return;
        }
        this.isRun = false;
        this.singleExecutor.shutdownNow();
        this.singleExecutor = null;
    }


    private void startCheckLocalDir(final String filePath) {
        final WatchService watcher = FileSystem.getDefault().newWatchService();

        Path path = new Path(new File(filePath));
        watcher.register(path, true, StandardWatchEventKind.ENTRY_CREATE, StandardWatchEventKind.ENTRY_DELETE,
            StandardWatchEventKind.ENTRY_MODIFY);
        checkAtFirst(watcher);
        singleExecutor.execute(new Runnable() {
            public void run() {
                log.debug(">>>>>>Begin monitor directory<<<<<<");
                while (isRun) {
                    WatchKey key;
                    try {
                        key = watcher.take();
                    }
                    catch (InterruptedException x) {
                        continue;
                    }
                    if (!processEvents(key)) {
                        log.error("reset invalid");
                        break;
                    }
                }
                log.debug(">>>>>>End monitor directory<<<<<<");
                watcher.close();

            }

        });
    }


    private void checkAtFirst(final WatchService watcher) {
        watcher.check();
        WatchKey key = null;
        while ((key = watcher.poll()) != null) {
            processEvents(key);
        }
    }


    @SuppressWarnings({ "unchecked" })
    private boolean processEvents(WatchKey key) {
        for (WatchEvent<?> event : key.pollEvents()) {

            WatchEvent<Path> ev = (WatchEvent<Path>) event;
            Path eventPath = ev.context();

            String realPath = eventPath.getAbsolutePath();
            if (ev.kind() == StandardWatchEventKind.ENTRY_CREATE || ev.kind() == StandardWatchEventKind.ENTRY_MODIFY) {

                String grandpaDir = null;
                try {
                    grandpaDir = FileUtils.getGrandpaDir(realPath);
                }
                catch (Exception e1) {

                }
                if (!Constants.BASE_DIR.equals(grandpaDir)) {
                    log.error("Illegal file enter directory: " + realPath);
                    continue;
                }
                existFiles.put(realPath, System.currentTimeMillis());
                if (log.isInfoEnabled()) {
                    log.info(realPath + "file added or updated");
                }
            }
            else if (ev.kind() == StandardWatchEventKind.ENTRY_DELETE) {
                String grandpaDir = null;
                try {
                    grandpaDir = FileUtils.getGrandpaDir(realPath);
                }
                catch (Exception e1) {

                }
                // delete file
                if (Constants.BASE_DIR.equals(grandpaDir)) {
                    existFiles.remove(realPath);
                    if (log.isInfoEnabled()) {
                        log.info(realPath + "file deleted");
                    }
                }
                else {
                    // delete directory
                    Set<String> keySet = new HashSet<String>(existFiles.keySet());
                    for (String filePath : keySet) {
                        if (filePath.startsWith(realPath)) {
                            existFiles.remove(filePath);
                            if (log.isInfoEnabled()) {
                                log.info(filePath + "file deleted");
                            }
                        }
                    }

                }
            }
        }
        return key.reset();
    }
}
