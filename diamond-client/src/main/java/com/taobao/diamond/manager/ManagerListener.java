/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.manager;

import java.util.concurrent.Executor;


/**
 * @author aoqiong
 */
public interface ManagerListener {

    public Executor getExecutor();

    public void receiveConfigInfo(final String configInfo);
}
