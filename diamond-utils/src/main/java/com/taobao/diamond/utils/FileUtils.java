/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.utils;

import com.taobao.diamond.common.Constants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;


public class FileUtils {

    public static boolean isFile(String path) {
        File file = new File(path);
        return file.isFile();
    }


    public static boolean isDirectory(String path) {
        File dir = new File(path);
        return dir.isDirectory();
    }


    public static String getFileName(String path) {
        File file = new File(path);
        if (!file.isFile()) {
            throw new RuntimeException("not file path=" + path);
        }
        return file.getName();
    }


    public static String getParentDir(String path) {
        File file = new File(path);
        if (!file.isFile()) {
            throw new RuntimeException("not file path=" + path);
        }
        File parent = file.getParentFile();
        if (parent.isDirectory()) {
            return parent.getName();
        } else {
            throw new RuntimeException("parent is not directory path=" + path);
        }
    }


    public static String getGrandpaDir(String path) {
        File file = new File(path);
        if (file.isDirectory()) {
            throw new RuntimeException("can't be directory path=" + path);
        }
        File parent = file.getParentFile();
        if (parent.isDirectory()) {
            File grandpa = parent.getParentFile();
            if (grandpa.isDirectory()) {
                return grandpa.getName();
            } else {
                throw new RuntimeException("grand parent is not directory path=" + path);
            }
        } else {
            throw new RuntimeException("parent is not directory path=" + path);
        }
    }


    public static String getFileContent(String path) throws IOException {
        File tFile = new File(path);
        if (!tFile.isFile()) {
            throw new RuntimeException("not file path=" + path);
        }
        RandomAccessFile file = new RandomAccessFile(tFile, "r");
        long fileSize = file.length();
        byte[] bytes = new byte[(int) fileSize];
        long readLength = 0L;
        while (readLength < fileSize) {
            int onceLength = file.read(bytes, (int) readLength, (int) (fileSize - readLength));
            if (onceLength > 0) {
                readLength += onceLength;
            } else {
                break;
            }
        }
        try {
            file.close();
        } catch (Exception e) {

        }
        return new String(bytes, Constants.ENCODE);
    }
}
