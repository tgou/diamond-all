/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.util;

/**
 * @author libinbin.pt
 * @filename PatternUtils.java
 * @datetime 2010-7-23 11:42:58
 */
public class PatternUtils {

    public static boolean hasCharPattern(String patternStr) {
        if (patternStr == null)
            return false;
        String pattern = patternStr;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '*')
                return true;
        }
        return false;
    }

    public static String generatePattern(String sourcePattern) {
        if (sourcePattern == null)
            return "";
        StringBuilder sb = new StringBuilder();
        String pattern = sourcePattern;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '*')
                sb.append('%');
            else
                sb.append(c);
        }
        return sb.toString();
    }

}
