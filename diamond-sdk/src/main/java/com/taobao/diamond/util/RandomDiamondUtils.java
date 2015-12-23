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

import com.taobao.diamond.domain.DiamondConf;

import java.util.List;
import java.util.Random;


public class RandomDiamondUtils {

    private List<DiamondConf> allDiamondConfs;
    private int retry_times;
    private int max_times;
    private int[] randomIndexSequence;
    private int currentIndex;

    public void init(List<DiamondConf> diamondConfs) {
        int len = diamondConfs.size();
        if (allDiamondConfs == null) {
            allDiamondConfs = diamondConfs;
        }
        max_times = len;
        retry_times = 0;
        currentIndex = 0;
        randomIndexSequence = new int[len];
        for (int i = 0; i < len; i++) {
            randomIndexSequence[i] = i;
        }
        if (len == 1)
            return;
        Random random = new Random();
        if (len == 2 && random.nextInt(2) == 1) {
            int temp = randomIndexSequence[0];
            randomIndexSequence[0] = randomIndexSequence[1];
            randomIndexSequence[1] = temp;
            return;
        }
        int times = 2 * len;
        for (int j = 0; j < times; j++) {
            int selectedIndex = random.nextInt(len - 1);
            int temp = randomIndexSequence[selectedIndex];
            randomIndexSequence[selectedIndex] = randomIndexSequence[len - 1];
            randomIndexSequence[len - 1] = temp;
        }
    }


    public int getRetry_times() {
        return retry_times;
    }

    public int getMax_times() {
        return max_times;
    }

    public DiamondConf generatorOneDiamondConf() {
        DiamondConf diamondConf = null;
        if (retry_times < max_times) {
            currentIndex = randomIndexSequence[retry_times];
            diamondConf = allDiamondConfs.get(currentIndex);
        } else {
            randomIndexSequence = null;
        }
        retry_times++;
        return diamondConf;
    }


    public int[] getRandomIndexSequence() {
        return randomIndexSequence;
    }

    public String getSequenceToString() {
        StringBuilder sb = new StringBuilder();
        for (int i : this.randomIndexSequence)
            sb.append(i + "");
        return sb.toString();
    }
}
