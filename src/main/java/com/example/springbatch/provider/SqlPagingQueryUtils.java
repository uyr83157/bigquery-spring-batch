package com.example.springbatch.provider;

import java.util.Map;

import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;

public final class SqlPagingQueryUtils {

	private SqlPagingQueryUtils() {}

	public static String buildOrderByClause(PagingQueryProvider provider) {
		return buildOrderByClause(provider.getSortKeys());
	}

	public static String buildOrderByClause(Map<String, Order> sortKeys) {
		if (sortKeys == null || sortKeys.isEmpty()) {
			return "";
		}
		StringBuilder orderBy = new StringBuilder("ORDER BY ");
		String sep = "";
		for (Map.Entry<String, Order> sortKey : sortKeys.entrySet()) {
			orderBy.append(sep)
				.append(sortKey.getKey())
				.append(" ")
				.append(sortKey.getValue() == Order.ASCENDING ? "ASC" : "DESC");
			sep = ", ";
		}
		return orderBy.toString();
	}
}
