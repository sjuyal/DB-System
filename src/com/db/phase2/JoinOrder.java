package com.db.phase2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

public class JoinOrder {

	private static long[][] a;
	private static int[][] s;
	private static TableDetails tables[][];
	private static int n;
	private static long cost;
	private static HashMap<String, HashSet<String>> joinPair;
	private static Vector<String> tableNames;

	public static class joinResult {
		String col1 = "";
		String col2 = "";
		Long totalRecords;
		Long minDistVal;

	}

	public static class TableDetails {
		long totalRecords;
		LinkedHashMap<String, Long> colDistPair = new LinkedHashMap<String, Long>();
	}

	public static void initialize(int noOfTables,
			HashMap<String, HashSet<String>> jP, Vector<String> tN) {
		n = noOfTables;
		joinPair = jP;
		// System.out.println("No of tables:" + noOfTables);
		tableNames = tN;
		a = new long[noOfTables + 1][noOfTables + 1];
		tables = new TableDetails[noOfTables + 1][noOfTables + 1];
		s = new int[noOfTables + 1][noOfTables + 1];

		for (int i = 1; i < noOfTables + 1; i++) {
			for (int j = 1; j < noOfTables + 1; j++) {
				TableDetails tD = new TableDetails();
				tables[i][j] = tD;
			}
		}
	}

	public static joinResult cost(int p, int q, int r, int s) {
		TableDetails tb1 = tables[p][q];
		TableDetails tb2 = tables[r][s];
		joinResult jR = new joinResult();

		long totalOutputRecords = 0;
		for (Map.Entry<String, Long> entry : tb1.colDistPair.entrySet()) {
			String colName = entry.getKey();
			Long distVal = entry.getValue();
			if (joinPair.containsKey(colName)) {
				HashSet<String> joinTo = joinPair.get(colName);
				for (Map.Entry<String, Long> entry2 : tb2.colDistPair
						.entrySet()) {
					String colName2 = entry2.getKey();
					Long distVal2 = entry2.getValue();
					if (joinTo.contains(colName2)) {

						totalOutputRecords = tb1.totalRecords
								* tb2.totalRecords;
						totalOutputRecords = (long) Math
								.ceil((double) totalOutputRecords
										/ Math.max(distVal, distVal2));
						// System.out.println("totalOutputRecords: "
						// + totalOutputRecords);
						jR.totalRecords = totalOutputRecords;
						jR.col1 = colName;
						jR.col2 = colName2;
						jR.minDistVal = Math.min(distVal, distVal2);
						return jR;
					}

				}
			}
		}
		jR.totalRecords = tb1.totalRecords * tb2.totalRecords;
		return jR;

	}

	public static void initBasicTableDetails(int i) {
		TableDetails tb = tables[i][i];
		// System.out.println("tb:" + tb);
		//System.out.println("---initBasicTableDetails");
		//System.out.println("TableName:" + tableNames.get(i));

		for (Map.Entry<String, String> entry : DBSystem.pageTree.get(tableNames
				.get(i)).coltypepair.entrySet()) {
			String colName = entry.getKey();
			long distinct = BPlusTree.bPlusTreeStructure.get(tableNames.get(i))
					.get(colName).distinctCols;
			tb.colDistPair.put(tableNames.get(i) + "." + colName, distinct);
			tb.totalRecords = DBSystem.pageTree.get(tableNames.get(i)).totalRecords;
			//System.out.println("ColName:" + colName + ":distinct:" + distinct
			//		+ ":records:" + tb.totalRecords);

		}
	}

	public static void updateTableDetails(int p, int q, int r, int s,
			joinResult jR) {
		TableDetails tb1 = tables[p][q];
		TableDetails tb2 = tables[r][s];
		TableDetails tb3 = tables[p][s];
		tb3.colDistPair.clear();
		tb3.totalRecords = jR.totalRecords;
		tb3.colDistPair.putAll(tb1.colDistPair);
		tb3.colDistPair.putAll(tb2.colDistPair);
		if (!jR.col1.equals("")) {
			tb3.colDistPair.put(jR.col1, jR.minDistVal);
			tb3.colDistPair.put(jR.col2, jR.minDistVal);
		}
	}

	private static void matrixChainOrder() {
		// deals with the empty sub problems
		for (int i = 1; i <= n; i++) {
			a[i][i] = 0;
			initBasicTableDetails(i);
		}

		// deals with chains of link 1
		for (int l = 2; l <= n; l++) {
			for (int i = 1; i <= n - l + 1; i++) {
				int j = i + l - 1;
				a[i][j] = Integer.MAX_VALUE;

				for (int k = i; k < j; k++) {
					joinResult jR = cost(i, k, k + 1, j);
					long q = a[i][k] + a[k + 1][j] + jR.totalRecords;
					if (q < a[i][j]) {
						a[i][j] = q;
						s[i][j] = k;
						updateTableDetails(i, k, k + 1, j, jR);
					}
				}
			}
		}
	}

	public static String printOptimalParens(int i, int j) {
		if (i == j) {
			// return "A[" + i + "]";
			return tableNames.get(i);
		} else
			return "(" + printOptimalParens(i, s[i][j]) + ","
					+ printOptimalParens(s[i][j] + 1, j) + ")";
	}

	public static void JoinOrderMain(HashMap<String, HashSet<String>> joinPair,
			Vector<String> tableNames) {
		//System.out.println("I am JoinOrderMain-------------");
		int numOfTables = tableNames.size() - 1;
		//System.out.println("initialize..........");
		JoinOrder.initialize(numOfTables, joinPair, tableNames);
		//System.out.println("matrixChainOrder..............");
		matrixChainOrder();
		long cost = a[1][numOfTables];
		System.out.println(printOptimalParens(1, numOfTables));
		System.out.println(cost);
	}
}
