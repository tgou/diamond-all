/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 * Authors:
 *   leiwen <chrisredfield1985@126.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.diamond.server.utils;

import com.taobao.diamond.domain.Page;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


/**
 * @author boyan
 * @date 2010-5-6
 * @param <E>
 */
public class PaginationHelper<E> {

    public Page<E> fetchPage(final JdbcTemplate jt, final String sqlCountRows, final String sqlFetchRows,
            final Object args[], final int pageNo, final int pageSize, final ParameterizedRowMapper<E> rowMapper) {
        if (pageSize == 0) {
            return null;
        }

        final int rowCount = jt.queryForInt(sqlCountRows, args);

        int pageCount = rowCount / pageSize;
        if (rowCount > pageSize * pageCount) {
            pageCount++;
        }

        final Page<E> page = new Page<E>();
        page.setPageNumber(pageNo);
        page.setPagesAvailable(pageCount);
        page.setTotalCount(rowCount);

        if (pageNo > pageCount)
            return null;
        final int startRow = (pageNo - 1) * pageSize;
        final String selectSQL = sqlFetchRows + " limit " + startRow + "," + pageSize;
        jt.query(selectSQL, args, new ResultSetExtractor() {
            public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                final List<E> pageItems = page.getPageItems();
                int currentRow = 0;
                while (rs.next()) {
                    pageItems.add(rowMapper.mapRow(rs, currentRow++));
                }
                return page;
            }
        });
        return page;
    }

}