/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.io.watch;

import com.taobao.diamond.io.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.concurrent.*;


/**
 * @author boyan
 * @date 2010-5-4
 */
public final class WatchService {
    private BlockingQueue<WatchKey> changedKeys = new LinkedBlockingQueue<WatchKey>();

    private BlockingQueue<WatchKey> watchedKeys = new LinkedBlockingQueue<WatchKey>();

    private static final Log log = LogFactory.getLog(WatchService.class);

    private ScheduledExecutorService service;


    public WatchService(long checkInterval) {
        service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new CheckThread(), checkInterval, checkInterval, TimeUnit.MILLISECONDS);
    }

    private final class CheckThread implements Runnable {
        public void run() {
            check();
        }

    }

    public void check() {
        synchronized (this) {
            Iterator<WatchKey> it = watchedKeys.iterator();
            while (it.hasNext()) {
                WatchKey key = it.next();
                try {
                    if (key.check()) {
                        changedKeys.add(key);
                        it.remove();
                    }
                } catch (Throwable t) {
                    log.error("WatchService check error,key=" + key, t);
                }
            }
        }
    }

    public WatchKey register(Path root, WatchEvent.Kind<?>... events) {
        if (events == null || events.length == 0)
            throw new UnsupportedOperationException("null events");
        if (this.service.isShutdown())
            throw new IllegalStateException("service has shutdown");
        if (!root.exists())
            throw new IllegalArgumentException("root not exist");
        WatchKey key = new WatchKey(root, this, false, events);
        resetKey(key);
        return key;
    }


    public WatchKey register(Path root, boolean fireCreatedEventOnIndex, WatchEvent.Kind<?>... events) {
        if (events == null || events.length == 0)
            throw new UnsupportedOperationException("null events");
        if (this.service.isShutdown())
            throw new IllegalStateException("service has shutdown");
        if (!root.exists())
            throw new IllegalArgumentException("root not exist");
        WatchKey key = new WatchKey(root, this, fireCreatedEventOnIndex, events);
        resetKey(key);
        return key;
    }


    boolean resetKey(WatchKey key) {
        return this.watchedKeys.add(key);
    }

    public void close() {
        this.service.shutdown();
    }

    public WatchKey poll() {
        return changedKeys.poll();
    }

    public WatchKey poll(long timeout, TimeUnit unit) throws InterruptedException {
        return changedKeys.poll(timeout, unit);
    }

    public WatchKey take() throws InterruptedException {
        return changedKeys.take();
    }
}
