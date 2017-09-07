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

import com.taobao.diamond.client.DiamondConfigure;

import java.util.List;
import java.util.Properties;


/**
 * @author aoqiong
 */
public interface DiamondManager {

    public void setManagerListener(ManagerListener managerListener);

    public void setManagerListeners(List<ManagerListener> managerListenerList);

    public List<ManagerListener> getManagerListeners();

    public String getConfigureInfomation(long timeout);

    public String getAvailableConfigureInfomation(long timeout);

    public String getAvailableConfigureInfomationFromSnapshot(long timeout);

    public Properties getPropertiesConfigureInfomation(long timeout);

    public Properties getAvailablePropertiesConfigureInfomationFromSnapshot(long timeout);

    public Properties getAvailablePropertiesConfigureInfomation(long timeout);

    public void setDiamondConfigure(DiamondConfigure diamondConfigure);

    public DiamondConfigure getDiamondConfigure();

    public void close();
}
