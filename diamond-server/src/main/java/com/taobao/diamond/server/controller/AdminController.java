/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.server.controller;

import com.taobao.diamond.common.Constants;
import com.taobao.diamond.domain.ConfigInfo;
import com.taobao.diamond.domain.ConfigInfoEx;
import com.taobao.diamond.domain.Page;
import com.taobao.diamond.server.exception.ConfigServiceException;
import com.taobao.diamond.server.service.AdminService;
import com.taobao.diamond.server.service.ConfigService;
import com.taobao.diamond.server.utils.DiamondUtils;
import com.taobao.diamond.server.utils.GlobalCounter;
import com.taobao.diamond.utils.JSONUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author boyan
 * @date 2010-5-6
 */
@Controller
@RequestMapping("/admin.do")
public class AdminController {

    private static final Log log = LogFactory.getLog(AdminController.class);

    @Autowired
    private AdminService adminService;

    @Autowired
    private ConfigService configService;


    @RequestMapping(params = "method=postConfig", method = RequestMethod.POST)
    public String postConfig(HttpServletRequest request, HttpServletResponse response,
                             @RequestParam("dataId") String dataId, @RequestParam("group") String group,
                             @RequestParam("content") String content, ModelMap modelMap) {
        response.setCharacterEncoding("UTF-8");

        boolean checkSuccess = true;
        String errorMessage = "Illegal Argument";
        if (StringUtils.isBlank(dataId) || DiamondUtils.hasInvalidChar(dataId.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal DataID";
        }
        if (StringUtils.isBlank(group) || DiamondUtils.hasInvalidChar(group.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal Group";
        }
        if (StringUtils.isBlank(content)) {
            checkSuccess = false;
            errorMessage = "Content can't be blank";
        }
        if (!checkSuccess) {
            modelMap.addAttribute("message", errorMessage);
            return "/admin/config/new";
        }

        dataId = dataId.trim();
        group = group.trim();

        this.configService.addConfigInfo(dataId, group, content);

        modelMap.addAttribute("message", "Submit success!");
        return listConfig(request, response, dataId, group, 1, 20, modelMap);
    }


    @RequestMapping(params = "method=deleteConfig", method = RequestMethod.GET)
    public String deleteConfig(HttpServletRequest request, HttpServletResponse response, @RequestParam("id") long id,
                               ModelMap modelMap) {
        this.configService.removeConfigInfo(id);
        modelMap.addAttribute("message", "Delete success!");
        return "/admin/config/list";
    }


    @RequestMapping(params = "method=upload", method = RequestMethod.POST)
    public String upload(HttpServletRequest request, HttpServletResponse response,
                         @RequestParam("dataId") String dataId, @RequestParam("group") String group,
                         @RequestParam("contentFile") MultipartFile contentFile, ModelMap modelMap) {
        response.setCharacterEncoding("UTF-8");

        boolean checkSuccess = true;
        String errorMessage = "Illegal Argument";
        if (StringUtils.isBlank(dataId) || DiamondUtils.hasInvalidChar(dataId.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal DataID";
        }
        if (StringUtils.isBlank(group) || DiamondUtils.hasInvalidChar(group.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal group";
        }
        String content = getContentFromFile(contentFile);
        if (StringUtils.isBlank(content)) {
            checkSuccess = false;
            errorMessage = "Content can't be blank";
        }
        if (!checkSuccess) {
            modelMap.addAttribute("message", errorMessage);
            return "/admin/config/upload";
        }

        this.configService.addConfigInfo(dataId, group, content);
        modelMap.addAttribute("message", "Submit Success!");
        return listConfig(request, response, dataId, group, 1, 20, modelMap);
    }


    @RequestMapping(params = "method=reupload", method = RequestMethod.POST)
    public String reupload(HttpServletRequest request, HttpServletResponse response,
                           @RequestParam("dataId") String dataId, @RequestParam("group") String group,
                           @RequestParam("contentFile") MultipartFile contentFile, ModelMap modelMap) {
        response.setCharacterEncoding("UTF-8");

        boolean checkSuccess = true;
        String errorMessage = "Illegal Argument";
        String content = getContentFromFile(contentFile);
        ConfigInfo configInfo = new ConfigInfo(dataId, group, content);
        if (StringUtils.isBlank(dataId) || DiamondUtils.hasInvalidChar(dataId.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal DataID";
        }
        if (StringUtils.isBlank(group) || DiamondUtils.hasInvalidChar(group.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal group";
        }
        if (StringUtils.isBlank(content)) {
            checkSuccess = false;
            errorMessage = "Content can't be blank";
        }
        if (!checkSuccess) {
            modelMap.addAttribute("message", errorMessage);
            modelMap.addAttribute("configInfo", configInfo);
            return "/admin/config/edit";
        }

        this.configService.updateConfigInfo(dataId, group, content);

        modelMap.addAttribute("message", "Update success!");
        return listConfig(request, response, dataId, group, 1, 20, modelMap);
    }


    private String getContentFromFile(MultipartFile contentFile) {
        try {
            String charset = Constants.ENCODE;
            final String content = new String(contentFile.getBytes(), charset);
            return content;
        } catch (Exception e) {
            throw new ConfigServiceException(e);
        }
    }


    @RequestMapping(params = "method=updateConfig", method = RequestMethod.POST)
    public String updateConfig(HttpServletRequest request, HttpServletResponse response,
                               @RequestParam("dataId") String dataId, @RequestParam("group") String group,
                               @RequestParam("content") String content, ModelMap modelMap) {
        response.setCharacterEncoding("UTF-8");

        ConfigInfo configInfo = new ConfigInfo(dataId, group, content);
        boolean checkSuccess = true;
        String errorMessage = "Illegal Argument";
        if (StringUtils.isBlank(dataId) || DiamondUtils.hasInvalidChar(dataId.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal DataID";
        }
        if (StringUtils.isBlank(group) || DiamondUtils.hasInvalidChar(group.trim())) {
            checkSuccess = false;
            errorMessage = "Illegal group";
        }
        if (StringUtils.isBlank(content)) {
            checkSuccess = false;
            errorMessage = "Content can't be blank";
        }
        if (!checkSuccess) {
            modelMap.addAttribute("message", errorMessage);
            modelMap.addAttribute("configInfo", configInfo);
            return "/admin/config/edit";
        }

        this.configService.updateConfigInfo(dataId, group, content);

        modelMap.addAttribute("message", "Submit success!");
        return listConfig(request, response, dataId, group, 1, 20, modelMap);
    }


    @RequestMapping(params = "method=listConfig", method = RequestMethod.GET)
    public String listConfig(HttpServletRequest request, HttpServletResponse response,
                             @RequestParam("dataId") String dataId, @RequestParam("group") String group,
                             @RequestParam("pageNo") int pageNo, @RequestParam("pageSize") int pageSize, ModelMap modelMap) {
        Page<ConfigInfo> page = this.configService.findConfigInfo(pageNo, pageSize, group, dataId);

        String accept = request.getHeader("Accept");
        if (accept != null && accept.indexOf("application/json") >= 0) {
            try {
                String json = JSONUtils.serializeObject(page);
                modelMap.addAttribute("pageJson", json);
            } catch (Exception e) {
                log.error("Serialize page object error:", e);
            }
            return "/admin/config/list_json";
        } else {
            modelMap.addAttribute("dataId", dataId);
            modelMap.addAttribute("group", group);
            modelMap.addAttribute("page", page);
            return "/admin/config/list";
        }
    }


    @RequestMapping(params = "method=listConfigLike", method = RequestMethod.GET)
    public String listConfigLike(HttpServletRequest request, HttpServletResponse response,
                                 @RequestParam("dataId") String dataId, @RequestParam("group") String group,
                                 @RequestParam("pageNo") int pageNo, @RequestParam("pageSize") int pageSize, ModelMap modelMap) {
        if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group)) {
            modelMap.addAttribute("message", "Must fill dataId or group when use like search");
            return "/admin/config/list";
        }
        Page<ConfigInfo> page = this.configService.findConfigInfoLike(pageNo, pageSize, group, dataId);

        String accept = request.getHeader("Accept");
        if (accept != null && accept.indexOf("application/json") >= 0) {
            try {
                String json = JSONUtils.serializeObject(page);
                modelMap.addAttribute("pageJson", json);
            } catch (Exception e) {
                log.error("Serialize page object error:", e);
            }
            return "/admin/config/list_json";
        } else {
            modelMap.addAttribute("page", page);
            modelMap.addAttribute("dataId", dataId);
            modelMap.addAttribute("group", group);
            modelMap.addAttribute("method", "listConfigLike");
            return "/admin/config/list";
        }
    }


    @RequestMapping(params = "method=detailConfig", method = RequestMethod.GET)
    public String getConfigInfo(HttpServletRequest request, HttpServletResponse response,
                                @RequestParam("dataId") String dataId, @RequestParam("group") String group, ModelMap modelMap) {
        dataId = dataId.trim();
        group = group.trim();
        ConfigInfo configInfo = this.configService.findConfigInfo(dataId, group);
        modelMap.addAttribute("configInfo", configInfo);
        return "/admin/config/edit";
    }


    // =========================== batch ============================== //

    @RequestMapping(params = "method=batchQuery", method = RequestMethod.POST)
    public String batchQuery(HttpServletRequest request, HttpServletResponse response,
                             @RequestParam("dataIds") String dataIds, @RequestParam("group") String group, ModelMap modelMap) {

        response.setCharacterEncoding("UTF-8");

        if (StringUtils.isBlank(dataIds)) {
            throw new IllegalArgumentException("batch query, dataIds can't be empty");
        }
        if (StringUtils.isBlank(group)) {
            throw new IllegalArgumentException("batch query, group can't be blank or contain illegal character");
        }

        String[] dataIdArray = dataIds.split(Constants.WORD_SEPARATOR);
        group = group.trim();

        List<ConfigInfoEx> configInfoExList = new ArrayList<ConfigInfoEx>();
        for (String dataId : dataIdArray) {
            ConfigInfoEx configInfoEx = new ConfigInfoEx();
            configInfoEx.setDataId(dataId);
            configInfoEx.setGroup(group);
            configInfoExList.add(configInfoEx);
            try {
                if (StringUtils.isBlank(dataId)) {
                    configInfoEx.setStatus(Constants.BATCH_QUERY_NONEXISTS);
                    configInfoEx.setMessage("dataId is blank");
                    continue;
                }

                ConfigInfo configInfo = this.configService.findConfigInfo(dataId, group);
                if (configInfo == null) {
                    configInfoEx.setStatus(Constants.BATCH_QUERY_NONEXISTS);
                    configInfoEx.setMessage("query data does not exist");
                } else {
                    String content = configInfo.getContent();
                    configInfoEx.setContent(content);
                    configInfoEx.setStatus(Constants.BATCH_QUERY_EXISTS);
                    configInfoEx.setMessage("query success");
                }
            } catch (Exception e) {
                log.error("batch query error, dataId=" + dataId + ",group=" + group, e);
                configInfoEx.setStatus(Constants.BATCH_OP_ERROR);
                configInfoEx.setMessage("query error: " + e.getMessage());
            }
        }

        String json = null;
        try {
            json = JSONUtils.serializeObject(configInfoExList);
        } catch (Exception e) {
            log.error("batch query serialize error, json=" + json, e);
        }
        modelMap.addAttribute("json", json);

        return "/admin/config/batch_result";
    }


    @RequestMapping(params = "method=batchAddOrUpdate", method = RequestMethod.POST)
    public String batchAddOrUpdate(HttpServletRequest request, HttpServletResponse response,
                                   @RequestParam("allDataIdAndContent") String allDataIdAndContent, @RequestParam("group") String group,
                                   ModelMap modelMap) {

        response.setCharacterEncoding("UTF-8");

        if (StringUtils.isBlank(allDataIdAndContent)) {
            throw new IllegalArgumentException("batch write, allDataIdAndContent can't be blank");
        }
        if (StringUtils.isBlank(group) || DiamondUtils.hasInvalidChar(group)) {
            throw new IllegalArgumentException("batch query, group can't be blank or contain illegal character");
        }

        String[] dataIdAndContentArray = allDataIdAndContent.split(Constants.LINE_SEPARATOR);
        group = group.trim();

        List<ConfigInfoEx> configInfoExList = new ArrayList<ConfigInfoEx>();
        for (String dataIdAndContent : dataIdAndContentArray) {
            String dataId = dataIdAndContent.substring(0, dataIdAndContent.indexOf(Constants.WORD_SEPARATOR));
            String content = dataIdAndContent.substring(dataIdAndContent.indexOf(Constants.WORD_SEPARATOR) + 1);
            ConfigInfoEx configInfoEx = new ConfigInfoEx();
            configInfoEx.setDataId(dataId);
            configInfoEx.setGroup(group);
            configInfoEx.setContent(content);

            try {
                if (StringUtils.isBlank(dataId) || DiamondUtils.hasInvalidChar(dataId)) {
                    throw new IllegalArgumentException("batch write, dataId can't be blank or contain illegal character");
                }
                if (StringUtils.isBlank(content)) {
                    throw new IllegalArgumentException("batch write, content can't be blank");
                }

                ConfigInfo configInfo = this.configService.findConfigInfo(dataId, group);
                if (configInfo == null) {
                    this.configService.addConfigInfo(dataId, group, content);
                    configInfoEx.setStatus(Constants.BATCH_ADD_SUCCESS);
                    configInfoEx.setMessage("add success");
                } else {
                    this.configService.updateConfigInfo(dataId, group, content);
                    configInfoEx.setStatus(Constants.BATCH_UPDATE_SUCCESS);
                    configInfoEx.setMessage("update success");
                }
            } catch (Exception e) {
                log.error("batch write error, dataId=" + dataId + ",group=" + group + ",content=" + content, e);
                configInfoEx.setStatus(Constants.BATCH_OP_ERROR);
                configInfoEx.setMessage("batch write error: " + e.getMessage());
            }
            configInfoExList.add(configInfoEx);
        }

        String json = null;
        try {
            json = JSONUtils.serializeObject(configInfoExList);
        } catch (Exception e) {
            log.error("batch write serialize error, json=" + json, e);
        }
        modelMap.addAttribute("json", json);

        return "/admin/config/batch_result";
    }


    @RequestMapping(params = "method=listUser", method = RequestMethod.GET)
    public String listUser(HttpServletRequest request, HttpServletResponse response, ModelMap modelMap) {
        Map<String, String> userMap = this.adminService.getAllUsers();
        modelMap.addAttribute("userMap", userMap);
        return "/admin/user/list";
    }


    @RequestMapping(params = "method=addUser", method = RequestMethod.POST)
    public String addUser(HttpServletRequest request, HttpServletResponse response,
                          @RequestParam("userName") String userName, @RequestParam("password") String password, ModelMap modelMap) {
        if (StringUtils.isBlank(userName) || DiamondUtils.hasInvalidChar(userName.trim())) {
            modelMap.addAttribute("message", "Illegal userName");
            return listUser(request, response, modelMap);
        }
        if (StringUtils.isBlank(password) || DiamondUtils.hasInvalidChar(password.trim())) {
            modelMap.addAttribute("message", "Illegal password");
            return "/admin/user/new";
        }
        if (this.adminService.addUser(userName, password))
            modelMap.addAttribute("message", "Add success!");
        else
            modelMap.addAttribute("message", "Add fail!");
        return listUser(request, response, modelMap);
    }


    @RequestMapping(params = "method=deleteUser", method = RequestMethod.GET)
    public String deleteUser(HttpServletRequest request, HttpServletResponse response,
                             @RequestParam("userName") String userName, ModelMap modelMap) {
        if (StringUtils.isBlank(userName) || DiamondUtils.hasInvalidChar(userName.trim())) {
            modelMap.addAttribute("message", "Illegal userName");
            return listUser(request, response, modelMap);
        }
        if (this.adminService.removeUser(userName)) {
            modelMap.addAttribute("message", "Delete success!");
        } else {
            modelMap.addAttribute("message", "Delete fail!");
        }
        return listUser(request, response, modelMap);
    }


    @RequestMapping(params = "method=changePassword", method = RequestMethod.GET)
    public String changePassword(HttpServletRequest request, HttpServletResponse response,
                                 @RequestParam("userName") String userName, @RequestParam("password") String password, ModelMap modelMap) {

        userName = userName.trim();
        password = password.trim();

        if (StringUtils.isBlank(userName) || DiamondUtils.hasInvalidChar(userName.trim())) {
            modelMap.addAttribute("message", "Illegal userName");
            return listUser(request, response, modelMap);
        }
        if (StringUtils.isBlank(password) || DiamondUtils.hasInvalidChar(password.trim())) {
            modelMap.addAttribute("message", "Illegal new password");
            return listUser(request, response, modelMap);
        }
        if (this.adminService.updatePassword(userName, password)) {
            modelMap.addAttribute("message", "Update success, please use the new password next login��");
        } else {
            modelMap.addAttribute("message", "Update fail!");
        }
        return listUser(request, response, modelMap);
    }


    @RequestMapping(params = "method=setRefuseRequestCount", method = RequestMethod.POST)
    public String setRefuseRequestCount(@RequestParam("count") long count, ModelMap modelMap) {
        if (count <= 0) {
            modelMap.addAttribute("message", "Illegal count");
            return "/admin/count";
        }
        GlobalCounter.getCounter().set(count);
        modelMap.addAttribute("message", "Set success!");
        return getRefuseRequestCount(modelMap);
    }


    @RequestMapping(params = "method=getRefuseRequestCount", method = RequestMethod.GET)
    public String getRefuseRequestCount(ModelMap modelMap) {
        modelMap.addAttribute("count", GlobalCounter.getCounter().get());
        return "/admin/count";
    }

    @RequestMapping(params = "method=reloadUser", method = RequestMethod.GET)
    public String reloadUser(HttpServletRequest request, HttpServletResponse response, ModelMap modelMap) {
        this.adminService.loadUsers();
        modelMap.addAttribute("message", "Load success!");
        return listUser(request, response, modelMap);
    }


    public AdminService getAdminService() {
        return adminService;
    }


    public void setAdminService(AdminService adminService) {
        this.adminService = adminService;
    }


    public ConfigService getConfigService() {
        return configService;
    }


    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

}
