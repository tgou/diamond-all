package com.taobao.diamond.server.service;

import com.taobao.diamond.common.Constants;
import com.taobao.diamond.domain.ConfigInfo;
import com.taobao.diamond.server.exception.ConfigServiceException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.util.WebUtils;

import javax.servlet.ServletContext;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author boyan
 * @date 2010-5-4
 */
@Service
public class DiskService {

    private static final Log log = LogFactory.getLog(DiskService.class);

    /**
     * modified cache
     */
    private final ConcurrentHashMap<String/* dataId + group */, Boolean/* modifying */> modifyMarkCache =
            new ConcurrentHashMap<String, Boolean>();

    @Autowired
    private ServletContext servletContext;


    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }


    public ServletContext getServletContext() {
        return this.servletContext;
    }


    /**
     * for unit test
     *
     * @return
     */
    public ConcurrentHashMap<String, Boolean> getModifyMarkCache() {
        return this.modifyMarkCache;
    }


    /**
     * for unit test
     *
     * @param dataId
     * @param group
     * @return
     * @throws FileNotFoundException
     */
    public String getFilePath(String dataId, String group) throws FileNotFoundException {
        return getFilePath(Constants.BASE_DIR + "/" + group + "/" + dataId);
    }


    public void saveToDisk(ConfigInfo configInfo) {
        String group = configInfo.getGroup();
        String dataId = configInfo.getDataId();
        String content = configInfo.getContent();
        String cacheKey = generateCacheKey(group, dataId);
        if (this.modifyMarkCache.putIfAbsent(cacheKey, true) == null) {
            File tempFile = null;
            try {
                String groupPath = getFilePath(Constants.BASE_DIR + "/" + group);
                createDirIfNessary(groupPath);
                File targetFile = createFileIfNessary(groupPath, dataId);
                tempFile = createTempFile(dataId, group);
                FileUtils.writeStringToFile(tempFile, content, Constants.ENCODE);
                FileUtils.copyFile(tempFile, targetFile);
            } catch (Exception e) {
                String errorMsg = "save disk error, dataId=" + dataId + ",group=" + group;
                log.error(errorMsg, e);
                throw new ConfigServiceException(errorMsg, e);
            } finally {
                if (tempFile != null && tempFile.exists()) {
                    FileUtils.deleteQuietly(tempFile);
                }
                this.modifyMarkCache.remove(cacheKey);
            }
        } else {
            throw new ConfigServiceException("config info is being modified, dataId=" + dataId + ",group=" + group);
        }

    }


    public boolean isModified(String dataId, String group) {
        return this.modifyMarkCache.get(generateCacheKey(group, dataId)) != null;
    }


    public final String generateCacheKey(String group, String dataId) {
        return group + "/" + dataId;
    }


    public void removeConfigInfo(String dataId, String group) {
        String cacheKey = generateCacheKey(group, dataId);
        if (this.modifyMarkCache.putIfAbsent(cacheKey, true) == null) {
            try {
                String basePath = getFilePath(Constants.BASE_DIR);
                createDirIfNessary(basePath);

                String groupPath = getFilePath(Constants.BASE_DIR + "/" + group);
                File groupDir = new File(groupPath);
                if (!groupDir.exists()) {
                    return;
                }

                String dataPath = getFilePath(Constants.BASE_DIR + "/" + group + "/" + dataId);
                File dataFile = new File(dataPath);
                if (!dataFile.exists()) {
                    return;
                }

                FileUtils.deleteQuietly(dataFile);
            } catch (Exception e) {
                String errorMsg = "delete config info error, dataId=" + dataId + ",group=" + group;
                log.error(errorMsg, e);
                throw new ConfigServiceException(errorMsg, e);
            } finally {
                this.modifyMarkCache.remove(cacheKey);
            }
        } else {
            throw new ConfigServiceException("config info is being modified, dataId=" + dataId + ",group=" + group);
        }
    }


    private String getFilePath(String dir) throws FileNotFoundException {
        return WebUtils.getRealPath(servletContext, dir);
    }


    private void createDirIfNessary(String path) {
        final File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }


    private File createFileIfNessary(String parent, String child) throws IOException {
        final File file = new File(parent, child);
        if (!file.exists()) {
            file.createNewFile();
            changeFilePermission(file);
        }
        return file;
    }


    private void changeFilePermission(File file) {
        // set file permission to 600
        file.setExecutable(false, false);
        file.setWritable(false, false);
        file.setReadable(false, false);
        file.setExecutable(false, true);
        file.setWritable(true, true);
        file.setReadable(true, true);
    }


    private File createTempFile(String dataId, String group) throws IOException {
        return File.createTempFile(group + "-" + dataId, ".tmp");
    }

}
