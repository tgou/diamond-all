/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.sdkapi;

import com.taobao.diamond.domain.*;

import java.util.List;
import java.util.Map;

/**
 * @author libinbin.pt
 * @filename DiamondSDKManager.java
 * @datetime 2010-7-16 04:03:28
 * <p/>
 * {@link #exists(String, String, String)}
 */
public interface DiamondSDKManager {

    Map<String, DiamondSDKConf> getDiamondSDKConfMaps();

    ContextResult publish(String dataId, String groupName,
                          String context, String serverId);

    ContextResult publishAfterModified(String dataId, String groupName,
                                       String context, String serverId);

    PageContextResult<ConfigInfo> queryBy(String dataIdPattern,
                                          String groupNamePattern, String serverId, long currentPage,
                                          long sizeOfPerPage);

    PageContextResult<ConfigInfo> queryBy(String dataIdPattern,
                                          String groupNamePattern, String contentPattern, String serverId,
                                          long currentPage, long sizeOfPerPage);

    ContextResult queryByDataIdAndGroupName(String dataId,
                                            String groupName, String serverId);

    ContextResult unpublish(String serverId, long id);

    BatchContextResult<ConfigInfoEx> batchQuery(String serverId, String groupName, List<String> dataIds);

    BatchContextResult<ConfigInfoEx> batchAddOrUpdate(String serverId, String groupName,
                                                      Map<String/* dataId */, String/* content */> dataId2ContentMap);

}
