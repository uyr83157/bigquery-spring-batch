package com.example.springbatch.provider;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.AbstractSqlPagingQueryProvider;
import org.springframework.util.Assert;

public class MySqlCustomPagingQueryProvider extends AbstractSqlPagingQueryProvider implements PagingQueryProvider {

	private final String baseSelectClause;
	private final String fromClause;
	private final String originalWhereClause;

	public MySqlCustomPagingQueryProvider(String baseSelectClause, String fromClause, String originalWhereClause) {
		Assert.hasText(baseSelectClause, "baseSelectClause 필수");
		Assert.hasText(fromClause, "fromClause 필수");
		Assert.hasText(originalWhereClause, "originalWhereClause 필수");

		this.baseSelectClause = baseSelectClause;
		this.fromClause = fromClause;
		this.originalWhereClause = originalWhereClause;

		super.setSelectClause(baseSelectClause);
		super.setFromClause(fromClause);
		super.setWhereClause(originalWhereClause);

		// Sort Keys 설정
		Map<String, Order> sortKeys = new LinkedHashMap<>();
		sortKeys.put("last_modified", Order.ASCENDING);
		sortKeys.put("auction_id", Order.ASCENDING);
		super.setSortKeys(sortKeys);
	}

	@Override
	public void init(DataSource dataSource) throws Exception {
		super.init(dataSource);
	}

	@Override
	public String generateFirstPageQuery(int pageSize) {
		String subQuery = String.format("SELECT %s FROM %s WHERE %s",
			this.baseSelectClause, this.fromClause, this.originalWhereClause);

		// 외부 쿼리 수정: "ORDER BY " 제거
		return String.format("SELECT * FROM (%s) AS derived_table %s LIMIT %d",
			subQuery, SqlPagingQueryUtils.buildOrderByClause(this), pageSize);
	}

	@Override
	public String generateRemainingPagesQuery(int pageSize) {
		String subQuery = String.format("SELECT %s FROM %s WHERE %s",
			this.baseSelectClause, this.fromClause, this.originalWhereClause);

		// 외부 쿼리 수정: "ORDER BY " 제거
		return String.format("SELECT * FROM (%s) AS derived_table WHERE %s %s LIMIT %d",
			subQuery, getOverClause(), SqlPagingQueryUtils.buildOrderByClause(this), pageSize);
	}


	private String getOverClause() {
		StringBuilder overClause = new StringBuilder();
		String HsqlOverClause = "(";
		String and = "";

		Map<String, Order> sortKeys = getSortKeys();
		Map<String, Boolean> isDate = new LinkedHashMap<>();

		int i = 0;
		for (Map.Entry<String, Order> sortKey : sortKeys.entrySet()) {
			String keyName = sortKey.getKey();
			if (i == 0) {
				HsqlOverClause += keyName + (sortKey.getValue() == Order.ASCENDING ? " > " : " < ") + ":_" + keyName;
			} else {
				StringBuilder equiClause = new StringBuilder();
				String and2 = "";
				int j = 0;
				for (Map.Entry<String, Order> prevSortKey : sortKeys.entrySet()) {
					if (j < i) {
						String prevKeyName = prevSortKey.getKey();
						equiClause.append(and2)
							.append(prevKeyName)
							.append(" = ")
							.append(":_")
							.append(prevKeyName);
						and2 = " AND ";
					} else {
						break;
					}
					j++;
				}
				overClause.append(" OR (")
					.append(equiClause)
					.append(" AND ")
					.append(keyName)
					.append(sortKey.getValue() == Order.ASCENDING ? " > " : " < ")
					.append(":_")
					.append(keyName)
					.append(")");
			}
			i++;
		}

		overClause.insert(0, HsqlOverClause);
		overClause.append(")");

		return overClause.toString();
	}
}

