/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * @param <E>
 * @author boyan
 * @date 2010-5-6
 */
public class Page<E> implements Serializable {
    static final long serialVersionUID = -1L;

    private int totalCount;
    private int pageNumber;
    private int pagesAvailable;
    private List<E> pageItems = new ArrayList<E>();


    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }


    public void setPagesAvailable(int pagesAvailable) {
        this.pagesAvailable = pagesAvailable;
    }


    public void setPageItems(List<E> pageItems) {
        this.pageItems = pageItems;
    }


    public int getTotalCount() {
        return totalCount;
    }


    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }


    public int getPageNumber() {
        return pageNumber;
    }


    public int getPagesAvailable() {
        return pagesAvailable;
    }


    public List<E> getPageItems() {
        return pageItems;
    }
}