package com.db.phase2;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TBaseType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TColumnDefinition;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TJoinItem;
import gudusoft.gsqlparser.nodes.TOrderByItemList;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import com.db.phase2.DBSystem.pageContent;

public class Main {

	static int flag = 0;
	static int pageSize;
	static int pageCount;
	static Integer temp;
	static HashMap<String, String[]> columnFlag;
	static LinkedList<String> invalidColumns;
	static HashMap<String, String> aliasTable;
	static HashMap<String, Boolean> duplicateColOrNot = new HashMap<String, Boolean>();
	static StringBuilder result = new StringBuilder();
	static String filename = "temp";
	static String finalfile = "result";
	static int filecount = 0;
	static File towrite;
	static FileWriter fw;
	static BufferedWriter bw;
	static String query;
	static String configPath;
	static TGSqlParser sqlparser;

	static FileWriter fd;
	static BufferedWriter bd;
	static BufferedReader br;
	static FileReader fr;

	static TOrderByItemList globTOrderByItemList;
	static HashMap<String, Integer> globHmp;
	static String globTableName;

	static String tableName1;
	static String tableName2;
	static String columnName1;
	static String columnName2;

	public static class JoinTableInfo {
		String joinWith;
		HashMap<String, String> joinColumns;
		HashMap<String, Integer> distinctValues = new HashMap<String, Integer>();
	}

	public static HashMap<String, JoinTableInfo> joinTable = new LinkedHashMap<String, JoinTableInfo>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			configPath = args[0];
			// System.out.println("Initialising..");
			DBSystem.readConfig(configPath);
			DBSystem.populateDBInfo();
			BPlusTree.intialiseBPTree();

			//System.out.println("Done Initialisation..");
			long time1 = System.currentTimeMillis();
			
			// DBSystem.insertRecord("student", "8,Z,Mtech");

			// for (Entry<String, tableInfo> entry :
			// DBSystem.pageTree.entrySet()) {
			// System.out.println("Table Name:" + entry.getKey());
			// for (Entry<String, String> colentry : DBSystem.pageTree
			// .get(entry.getKey()).coltypepair.entrySet()) {
			// System.out.println("Column Name:" + colentry.getKey());
			// System.out.println((BPlusTree.bPlusTreeStructure.get(
			// entry.getKey()).get(colentry.getKey()).toString()));
			// // System.out.println("MAX:"
			// // + BPlusTree.bPlusTreeStructure.get(entry.getKey())
			// // .get(colentry.getKey()).max);
			// // System.out.println("MIN:"
			// // + BPlusTree.bPlusTreeStructure.get(entry.getKey())
			// // .get(colentry.getKey()).min);
			// System.out.println("Distinct:"
			// + DBSystem.V(entry.getKey(), colentry.getKey()));
			// }
			// }

			// SortMergeJoin.sortMergeMain("./", "student", "Roll", "employee1",
			// "NAME");
			// SortMergeJoin.sortMergeMain("./", "employee1", "ID", "employee2",
			// "ID");

			Scanner in = new Scanner(System.in);
			int len = Integer.parseInt(in.nextLine());
			for (int i = 0; i < len; i++) {
				query = in.nextLine();
				if (checkValidity(query)) {
					processQuery();
				} else {
					System.out.println("Query Invalid");
				}
			}

			long time2 = System.currentTimeMillis();
			// System.out.println((time2-time1)/1000.0);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void innerJoin(String table1, String col1, String table2,
			String col2) {
		// col1 = "employee1.ID";
		col1 = columnFlag.get(col1)[2];
		// col2 = "employee2.ID";
		col2 = columnFlag.get(col2)[2];
		SortMergeJoin.sortMergeMain(DBSystem.pathForData, table1, col1, table2,
				col2);
		System.out.println("Done!!");
	}

	static void processQuery() {
		// sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		// sqlparser.sqltext = query;

		flag = 0;
		// int ret = sqlparser.parse();
		// if (ret == 0) {
		for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
			processAnalyzeStmt(sqlparser.sqlstatements.get(i));
		}
		// }
	}

	private static void processAnalyzeStmt(TCustomSqlStatement stmt) {
		flag = 0;
		switch (stmt.sqlstatementtype) {
		case sstselect:
			processSelectStmt((TSelectSqlStatement) stmt);
			break;
		case sstupdate:
			break;
		case sstcreatetable:
			// processCreateTableStmt((TCreateTableSqlStatement) stmt);
			break;
		case sstaltertable:
			break;
		case sstcreateview:
			break;
		default:
			System.out.println(stmt.sqlstatementtype.toString());
		}
	}

	protected static void processSelectStmt(TSelectSqlStatement pStmt) {

		// System.out.println("Querytype:select");
		HashSet<String> h;
		TreeSet<Integer> tree = new TreeSet<Integer>();
		String tableName = pStmt.joins.getJoin(0).getTable().toString();
		if (pStmt.isCombinedQuery()) {
			String setstr = "";
			switch (pStmt.getSetOperator()) {
			case 1:
				setstr = "union";
				break;
			case 2:
				setstr = "union all";
				break;
			case 3:
				setstr = "intersect";
				break;
			case 4:
				setstr = "intersect all";
				break;
			case 5:
				setstr = "minus";
				break;
			case 6:
				setstr = "minus all";
				break;
			case 7:
				setstr = "except";
				break;
			case 8:
				setstr = "except all";
				break;
			}
			System.out.printf("set type: %s\n", setstr);
			System.out.println("left select:");
			analyzeSelectStmt(pStmt.getLeftStmt());
			System.out.println("right select:");
			analyzeSelectStmt(pStmt.getRightStmt());
			if (pStmt.getOrderbyClause() != null) {
				System.out.printf("order by clause%s\n", pStmt
						.getOrderbyClause().toString());
			}
		} else {
			for (int i = 0; i < pStmt.joins.size(); i++) {

				TJoin join = pStmt.joins.getJoin(i);
				switch (join.getKind()) {
				case TBaseType.join_source_fake:
					break;
				case TBaseType.join_source_table:
					Vector<String> tableNames = new Vector<String>();
					TJoinItem joinItem = join.getJoinItems().getJoinItem(0);
					if (join.getJoinItems().size() == 1) {
						String table1 = join.getTable().toString().trim();
						String table2 = joinItem.getTable().toString().trim();
						String left = joinItem.getOnCondition()
								.getLeftOperand().toString();
						String right = joinItem.getOnCondition()
								.getRightOperand().toString();
						String leftTable = columnFlag.get(left)[1];
						String rightTable = columnFlag.get(right)[1];
						left = columnFlag.get(left)[2].trim();
						right = columnFlag.get(right)[2].trim();
						
						if(!leftTable.equals(table1)){
							String temp =leftTable;
							leftTable = rightTable;
							rightTable = temp;
							temp =left;
							left = right;
							right = temp;
						}
						
						SortMergeJoin.sortMergeMain(DBSystem.pathForData,
								leftTable, left, rightTable, right);
						
						SortMergeJoin.displayResults(pStmt, SortMergeJoin.finalFileName, leftTable, rightTable);
						return;
					} else {
						tableNames.add("$$");
						HashMap<String, HashSet<String>> joinPair = new HashMap<String, HashSet<String>>();
						tableNames.add(join.getTable().toString().trim());
						for (int j = 0; j < join.getJoinItems().size(); j++) {
							joinItem = join.getJoinItems().getJoinItem(j);
							tableNames.add(joinItem.getTable().toString()
									.trim());
							fillJoinAttributes(joinItem.getOnCondition(), null,
									null, joinPair);
							// System.out.println("1--");
						}
						// for (String s : tableNames) {
						// System.out.println(s);
						// }
						JoinOrder.JoinOrderMain(joinPair, tableNames);
						return;
					}

				}
			}

			TResultColumn resultColumn = null;
			for (int i = 0; i < pStmt.getResultColumnList().size(); i++) {
				resultColumn = pStmt.getResultColumnList().getResultColumn(i);
			}
			// System.out.print("Condition:");
			if (pStmt.getWhereClause() != null) {
				h = processWhereHaving(pStmt.getWhereClause().getCondition());
				tree = new TreeSet<Integer>();
				// System.out.println(h);
				if (h.contains("-2"))
					h.clear();
				for (String str : h) {
					// System.out.println("$$" + str);
					tree.add(Integer.parseInt(str));
				}
				TJoin join = pStmt.joins.getJoin(0);
				tableName = join.getTable().toString();
				String record;
				/*
				 * for(Integer s:tree){ try { record =
				 * DBSystem.getRecord(tableName,s);
				 * System.out.println("$$"+record+"$$"); } catch
				 * (NumberFormatException | IOException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); } }
				 * System.out.println();
				 */
			} else {
				for (int i = 0; i < DBSystem.pageTree.get(tableName).totalRecords; i++)
					tree.add(i);
			}

			if (pStmt.getOrderbyClause() != null) {
				processOrderByClause(tableName, tree, pStmt.getOrderbyClause()
						.getItems());
			} else
				processWithoutOrderByClause(tableName, tree);
			if (pStmt.getGroupByClause() != null) {
			}

			if (pStmt.getGroupByClause() != null) {
				if (pStmt.getGroupByClause().getHavingClause() != null) {
					processWhereHaving(pStmt.getGroupByClause()
							.getHavingClause());
				}
			}
			if (pStmt.getForUpdateClause() != null) {
			}
			// top clause
			if (pStmt.getTopClause() != null) {
			}
			// limit clause
			if (pStmt.getLimitClause() != null) {
			}

			displayResults(pStmt, finalfile, tableName);
		}
	}

	private static void processWithoutOrderByClause(String tableName,
			TreeSet<Integer> tree) {
		try {
			fd = new FileWriter(DBSystem.pathForData + "/" + finalfile);
			bd = new BufferedWriter(fd);
			// System.out.println("finalfile:"+finalfile+" tablename:"+tableName);
			for (Integer line : tree) {
				// System.out.println("$$" + line);
				String s = DBSystem.getRecord(tableName, line);
				bd.write(s + "\n");
			}
			bd.close();
			fd.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub

	}

	private static HashSet<String> processWhereHaving(TExpression condition) {

		// System.out.println("$$$" + condition);

		if (condition != null && condition.getLeftOperand() == null
				&& condition.getRightOperand() == null) {
			HashSet<String> h = new HashSet<>();
			h.add("-3");
			// System.out.println("^^^^^^^^^^^^$" + condition);
			return h;
		}
		HashSet<String> h1 = null, h2 = null;
		if (condition.getOperatorToken() == null
				|| condition.getOperatorToken().toString()
						.equalsIgnoreCase("and")
				|| condition.getOperatorToken().toString()
						.equalsIgnoreCase("or")) {

			// System.out.println("Condition:"+condition+":left:"+condition.getLeftOperand()+":Operator:"+condition.getOperatorToken()+":Right:"+condition.getRightOperand());

			if (condition.getLeftOperand() != null) {
				h2 = processWhereHaving(condition.getLeftOperand());
				// System.out.println("Getting:"+h2+":condition:"+condition);
			}
			if (condition.getRightOperand() != null) {
				h1 = processWhereHaving(condition.getRightOperand());
				// System.out.println("Getting:"+h1+":condition:"+condition);
			}

			if (condition.getOperatorToken() == null) {
				HashSet<String> h = new HashSet<>();
				if (condition.getLeftOperand() != null
						&& condition.getRightOperand() == null)
					return h2;
				if (condition.getLeftOperand() == null
						&& condition.getRightOperand() != null)
					return h1;
				h.add("-3");
				// System.out.println("$$$" + condition);
				return h;
			}

			if (condition.getOperatorToken().toString().equalsIgnoreCase("and")) {
				// System.out.println("&&&&"+condition);
				// System.out.println("H1:"+h1);
				// System.out.println("H2:"+h2);
				if (h1.contains("-1") || h1.contains("-3"))
					return h2;
				if (h2.contains("-1") || h2.contains("-3"))
					return h1;
				if (h1.contains("-2")) {
					// System.out.println("------");
					return h1;
				}
				if (h2.contains("-2")) {

					return h2;
				}
				h1.retainAll(h2);
				// System.out.println("Return:"+h1);
				return h1;
			}
			if (condition.getOperatorToken().toString().equalsIgnoreCase("or")) {
				if (h1.contains("-2") || h1.contains("-3"))
					return h2;
				if (h2.contains("-2") || h2.contains("-3"))
					return h1;
				if (h1.contains("-1"))
					return h2;
				if (h2.contains("-1"))
					return h1;
				h1.addAll(h2);
				return h1;
			}
		} else {

			if (condition.getLeftOperand() != null
					&& condition.getRightOperand() != null) {
				// System.out.println("$$"+condition.getLeftOperand()
				// +"$$"+condition.getOperatorToken()+"$$"+condition.getRightOperand()+"$$");

				return performBasicOperations(condition);

			} else {
				HashSet<String> h = new HashSet<>();
				h.add("-3");
				// System.out.println("$$$" + condition);
				return h;
			}
		}
		HashSet<String> h = new HashSet<>();
		h.add("-3");

		return h;
	}

	private static void displayResults(TSelectSqlStatement pStmt,
			String finalfile, String tableName) {
		try {
			int h = 0;
			HashMap<String, Integer> hmp = new HashMap<String, Integer>();
			Iterator<Entry<String, String>> itr = DBSystem.pageTree
					.get(tableName).coltypepair.entrySet().iterator();
			while (itr.hasNext()) {
				hmp.put(itr.next().getKey(), h++);
			}
			fr = new FileReader(DBSystem.pathForData + "/" + finalfile);
			br = new BufferedReader(fr);
			String line;
			TResultColumn resultColumn = pStmt.getResultColumnList()
					.getResultColumn(0);
			if (resultColumn.getExpr().toString().equalsIgnoreCase("*")) {
				int length = 0;
				Set<String> columnnames = DBSystem.pageTree.get(tableName).coltypepair
						.keySet();
				for (String column : columnnames) {
					if (length < columnnames.size() - 1) {
						System.out.print("\"" + column + "\",");
					} else {
						System.out.println("\"" + column + "\"");
					}
					length++;
				}
				while ((line = br.readLine()) != null) {
					// System.out.println("---" + line);
					String tok[] = DBSystem.splitTableRow(line);
					int i = 0;
					for (i = 0; i < tok.length - 1; i++) {
						System.out.print("\"" + tok[i] + "\",");
					}
					System.out.println("\"" + tok[i] + "\"");
				}
			} else {

				int i;
				for (i = 0; i < pStmt.getResultColumnList().size() - 1; i++) {
					System.out.print("\""
							+ pStmt.getResultColumnList().getResultColumn(i)
							+ "\",");
				}
				System.out
						.println("\""
								+ pStmt.getResultColumnList()
										.getResultColumn(i) + "\"");

				while ((line = br.readLine()) != null) {
					// System.out.println("---" + line);
					String l[] = DBSystem.splitTableRow(line);
					for (i = 0; i < pStmt.getResultColumnList().size() - 1; i++) {
						resultColumn = pStmt.getResultColumnList()
								.getResultColumn(i);
						System.out.print("\""
								+ l[hmp.get(resultColumn.toString().trim())]
								+ "\",");
					}
					resultColumn = pStmt.getResultColumnList().getResultColumn(
							i);
					System.out
							.println("\""
									+ l[hmp.get(resultColumn.toString().trim())]
									+ "\"");
				}
			}
			br.close();
			fr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void processOrderByClause(String tableName,
			TreeSet<Integer> tree, TOrderByItemList tOrderByItemList) {
		// TODO Auto-generated method stub

		/*
		 * for (int i = 0; i < tOrderByItemList.size(); i++) {
		 * System.out.println("-----------" +
		 * tOrderByItemList.getOrderByItem(i)); }
		 */
		towrite = new File(DBSystem.pathForData + "/" + filename + filecount);
		try {
			fw = new FileWriter(towrite);
		} catch (IOException e) {
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		int h = 0;
		HashMap<String, Integer> hmp = new HashMap<String, Integer>();
		Iterator<Entry<String, String>> itr = DBSystem.pageTree.get(tableName).coltypepair
				.entrySet().iterator();
		while (itr.hasNext()) {
			hmp.put(itr.next().getKey(), h++);
		}

		ArrayList<Integer> prunedTree = new ArrayList<Integer>();
		for (Integer s : tree) {
			prunedTree.add(s);
			boolean ret = LRUPlug.consecutiveInserts(tableName, s);
			// DBSystem.printLRU();
			if (!ret) {
				// System.out.println("Dumpppppppppp iiiiiiiinnnnnntoooo");
				prunedTree.remove(prunedTree.size() - 1);
				dumpIntoFile(tableName, LRUPlug.getVectorOfRecords(),
						prunedTree, tOrderByItemList, hmp);
				LRUPlug.initializer();
				prunedTree.clear();
				prunedTree.add(s);
				LRUPlug.consecutiveInserts(tableName, s);
			}
		}
		if (LRUPlug.flag == 1)
			dumpIntoFile(tableName, LRUPlug.getVectorOfRecords(), prunedTree,
					tOrderByItemList, hmp);
		// mergeFiles(tableName,tOrderByItemList,hmp);
		ExternalMergeSort ext = new ExternalMergeSort(filecount, filecount);
		ext.initialiseMerge(DBSystem.pathForData, filename, finalfile,
				tableName, tOrderByItemList, hmp);
		// DBSystem.printLRU();
	}

	public static void dumpIntoFile(String tableName,
			Vector<String> recordVector, ArrayList<Integer> tree,
			TOrderByItemList tOrderByItemList, HashMap<String, Integer> hmp) {
		// System.out.println("********************************");
		try {
			String min;
			pageContent pc, currentpc;
			String comp, comp1;

			/*
			 * for (int i = 0; i < tree.size(); i++) {
			 * System.out.println("Treee pRuned:" + tree.get(i)); }
			 */
			/*
			 * System.out.println("Before Sortinngggggggggggggggggg"); for (int
			 * i = 0; i < recordVector.size(); i++) {
			 * System.out.println("Vector:" + recordVector.get(i)); }
			 */

			// sort my vector------------->
			// callSort(tableName,recordVector);

			Comparator<String> cmp = new Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					// TODO Auto-generated method stub
					return (compareRecords(globTableName, s1, s2,
							globTOrderByItemList, globHmp));
				}

			};

			initialiseCompRecordGlobalParams(tableName, tOrderByItemList, hmp);
			Collections.sort(recordVector, cmp);

			/*
			 * System.out.println("After Sortinngggggggggggggggggg"); for (int i
			 * = 0; i < recordVector.size(); i++) { System.out.println("Vector:"
			 * + recordVector.get(i)); }
			 */
			for (int i = 0; i < recordVector.size(); i++) {
				FileWrite(recordVector.get(i));
			}
			filecount++;
			bw.close();
			fw.close();
			towrite = new File(DBSystem.pathForData + "/" + filename
					+ filecount);
			try {
				fw = new FileWriter(towrite);
			} catch (IOException e) {
				e.printStackTrace();
			}
			bw = new BufferedWriter(fw);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void initialiseCompRecordGlobalParams(String tableName2,
			TOrderByItemList tOrderByItemList2, HashMap<String, Integer> hmp2) {
		globTableName = tableName2;
		globTOrderByItemList = tOrderByItemList2;
		globHmp = hmp2;

	}

	static int compareRecords(String tableName, String s1, String s2,
			TOrderByItemList tOrderByItemList, HashMap<String, Integer> hmp) {

		// System.out.println("--" + s1);
		// System.out.println("--" + s2);
		String split1[] = DBSystem.splitTableRow(s1);
		String split2[] = DBSystem.splitTableRow(s2);
		String comp1, comp2;

		int result = 0;
		for (int l = 0; l < tOrderByItemList.size(); l++) {
			// System.out.println("-----------"
			// +tOrderByItemList.getOrderByItem(l)+"\nhmp:"+hmp.get(tOrderByItemList.getOrderByItem(l).toString().trim()));
			comp1 = split1[hmp.get(tOrderByItemList.getOrderByItem(l)
					.toString().trim())].trim();
			comp2 = split2[hmp.get(tOrderByItemList.getOrderByItem(l)
					.toString().trim())];

			result = compare(comp1, comp2,
					DBSystem.pageTree.get(tableName).coltypepair
							.get(tOrderByItemList.getOrderByItem(l).toString()
									.trim()));
			if (result != 0) {
				return result;
			}
		}
		return -1;
	}

	static void mergeFiles(String tableName, TOrderByItemList tOrderByItemList,
			HashMap<String, Integer> hmp) {
		try {
			File toread[] = new File[filecount];
			for (int i = 0; i < filecount; i++) {
				toread[i] = new File(filename + i);
			}

			FileReader fr[] = new FileReader[filecount];
			for (int i = 0; i < filecount; i++) {
				fr[i] = new FileReader(toread[i]);
			}

			BufferedReader br[] = new BufferedReader[filecount];
			for (int i = 0; i < filecount; i++) {
				br[i] = new BufferedReader(fr[i]);
			}

			String line[] = new String[filecount];
			for (int i = 0; i < filecount; i++) {
				line[i] = br[i].readLine();
			}

			String min = "";
			int minIndex;

			for (int i = 0; i < line.length; i++) {
				if (min.equals("")) {
					minIndex = i;
					min = line[i];
				} else {
					int result = compareRecords(tableName, min, line[i],
							tOrderByItemList, hmp);
					if (result > 0) {
						minIndex = i;
						min = line[i];
					}
				}
			}

			for (int i = 0; i < filecount; i++) {
				br[i].close();
				fr[i].close();
			}
			filecount = 0;
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	static void FileWrite(String comp) {
		try {
			bw.write(comp);
			bw.newLine();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static String extractDataType(String operand) {
		String dataType;
		if (columnFlag.containsKey(operand)) {
			dataType = columnFlag.get(operand)[0];
			dataType = dataType.trim();
			String chk = new String(dataType.toCharArray(), 0, 3);
			if (chk.equalsIgnoreCase("var"))
				dataType = "varchar";
			// System.out.println(rightOperand+":"+dataTypeRight);
		} else {
			try {
				Integer.parseInt(operand);
				dataType = "integer";
			} catch (NumberFormatException e) {
				// System.out.println(rightOperand+" is not a integer");
				try {
					Float.parseFloat(operand);
					dataType = "float";
				} catch (NumberFormatException e1) {
					// System.out.println(rightOperand+" is not a float");
					char rightarray[] = operand.toCharArray();
					if (rightarray[0] == '\''
							&& rightarray[rightarray.length - 1] == '\'') {
						dataType = "varchar";
					} else {
						dataType = "#";
					}
				}
			}
		}
		dataType = dataType.trim();
		return dataType;
	}

	public static int compare(String s1, String s2, String coltype) {
		// System.out.println("^^^^^^^" + s1);
		// System.out.println("%%%%%%%" + s2);
		if (coltype.length() >= 7
				&& coltype.substring(0, 7).equalsIgnoreCase("varchar"))
			return (s1.compareTo(s2));
		else {
			// System.out.println("Type:" + coltype + " Val1:" + s1 + " Val2:" +
			// s2);
			double l1 = Double.parseDouble(s1);
			double l2 = Double.parseDouble(s2);
			if (l1 > l2)
				return 1;
			else if (l1 < l2)
				return -1;
			else
				return 0;
		}
	}

	static HashSet<String> performBasicOperations(TExpression condition) {
		String leftOperand = condition.getLeftOperand().toString();
		String rightOperand = condition.getRightOperand().toString();
		String dataTypeLeft, dataTypeRight;
		dataTypeLeft = extractDataType(leftOperand);
		dataTypeRight = extractDataType(rightOperand);
		HashSet<String> ret = new HashSet<String>();
		// System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+condition.toString());
		int lflag = 0, rflag = 0;

		if (columnFlag.containsKey(leftOperand)) {
			lflag++;
		}
		if (columnFlag.containsKey(rightOperand)) {
			rflag++;
		}
		if (leftOperand.charAt(0) == '\'')
			leftOperand = leftOperand.substring(1, leftOperand.length() - 1);
		if (rightOperand.charAt(0) == '\'')
			rightOperand = rightOperand.substring(1, rightOperand.length() - 1);
		switch (condition.getOperatorToken().toString().toLowerCase()) {
		case "=":
			if (lflag == 0 && rflag == 0) {
				if (compare(leftOperand, rightOperand, dataTypeLeft) == 0) {
					ret.add("-1");
					return ret;
				} else {
					ret.add("-2");
					return ret;
				}
			} else if (lflag == 1 && rflag == 0) {
				// System.out.println("a----------------------------------------------------");
				ret = callSearch(columnFlag.get(leftOperand)[1], leftOperand,
						rightOperand);

			} else if (lflag == 0 && rflag == 1) {
				ret = callSearch(columnFlag.get(rightOperand)[1], rightOperand,
						leftOperand);
			} else if (lflag == 1 && rflag == 1) {
				ret = equateColumns(columnFlag.get(rightOperand)[1],
						leftOperand, rightOperand, "=");
			}
			break;
		case "!=":
			if (lflag == 0 && rflag == 0) {
				if (compare(leftOperand, rightOperand, dataTypeLeft) != 0) {
					ret.add("-1");
					return ret;
				} else {
					ret.add("-2");
					return ret;
				}
			} else if (lflag == 1 && rflag == 0) {
				ret = callNotEqualTo(columnFlag.get(leftOperand)[1],
						leftOperand, rightOperand);

			} else if (lflag == 0 && rflag == 1) {
				ret = callNotEqualTo(columnFlag.get(rightOperand)[1],
						rightOperand, leftOperand);
			} else if (lflag == 1 && rflag == 1) {
				ret = equateColumns(columnFlag.get(rightOperand)[1],
						leftOperand, rightOperand, "!=");
			}
			break;
		case "<=":
			if (lflag == 0 && rflag == 0) {
				if (compare(leftOperand, rightOperand, dataTypeLeft) <= 0) {
					ret.add("-1");
					return ret;
				} else {
					ret.add("-2");
					return ret;
				}
			} else if (lflag == 1 && rflag == 0) {
				ret = callRangeSearch(columnFlag.get(leftOperand)[1],
						leftOperand, null, rightOperand, true, true);

			} else if (lflag == 0 && rflag == 1) {
				ret = callRangeSearch(columnFlag.get(rightOperand)[1],
						rightOperand, leftOperand, null, true, true);
			} else if (lflag == 1 && rflag == 1) {
				ret = equateColumns(columnFlag.get(rightOperand)[1],
						leftOperand, rightOperand, "<=");
			}
			break;
		case ">=":
			if (lflag == 0 && rflag == 0) {
				if (compare(leftOperand, rightOperand, dataTypeLeft) >= 0) {
					ret.add("-1");
					return ret;
				} else {
					ret.add("-2");
					return ret;
				}
			} else if (lflag == 1 && rflag == 0) {
				ret = callRangeSearch(columnFlag.get(leftOperand)[1],
						leftOperand, rightOperand, null, true, true);

			} else if (lflag == 0 && rflag == 1) {
				ret = callRangeSearch(columnFlag.get(rightOperand)[1],
						rightOperand, null, leftOperand, true, true);
			} else if (lflag == 1 && rflag == 1) {
				ret = equateColumns(columnFlag.get(rightOperand)[1],
						leftOperand, rightOperand, ">=");
			}
			break;
		case ">":
			if (lflag == 0 && rflag == 0) {
				if (compare(leftOperand, rightOperand, dataTypeLeft) > 0) {
					ret.add("-1");
					return ret;
				} else {
					ret.add("-2");
					return ret;
				}
			} else if (lflag == 1 && rflag == 0) {
				ret = callRangeSearch(columnFlag.get(leftOperand)[1],
						leftOperand, rightOperand, null, false, true);

			} else if (lflag == 0 && rflag == 1) {
				ret = callRangeSearch(columnFlag.get(rightOperand)[1],
						rightOperand, null, leftOperand, false, true);
			} else if (lflag == 1 && rflag == 1) {
				ret = equateColumns(columnFlag.get(rightOperand)[1],
						leftOperand, rightOperand, ">");
			}
			break;
		case "<":
			if (lflag == 0 && rflag == 0) {
				if (compare(leftOperand, rightOperand, dataTypeLeft) < 0) {
					ret.add("-1");
					return ret;
				} else {
					ret.add("-2");
					return ret;
				}
			} else if (lflag == 1 && rflag == 0) {
				ret = callRangeSearch(columnFlag.get(leftOperand)[1],
						leftOperand, null, rightOperand, true, false);

			} else if (lflag == 0 && rflag == 1) {
				ret = callRangeSearch(columnFlag.get(rightOperand)[1],
						rightOperand, leftOperand, null, true, false);
			} else if (lflag == 1 && rflag == 1) {
				ret = equateColumns(columnFlag.get(rightOperand)[1],
						leftOperand, rightOperand, "<");
			}
			break;
		case "like":
			ret = callSearch(columnFlag.get(leftOperand)[1], leftOperand,
					rightOperand);
			/*
			 * if (lflag == 0 && rflag == 0) { if (compare(leftOperand,
			 * rightOperand, dataTypeLeft) == 0) { ret.add("-1"); return ret; }
			 * else { ret.add("-2"); return ret; } } else if (lflag == 1 &&
			 * rflag == 0) { ret = callSearch(columnFlag.get(leftOperand)[1],
			 * leftOperand, rightOperand);
			 * 
			 * } else if (lflag == 0 && rflag == 1) { ret =
			 * callSearch(columnFlag.get(rightOperand)[1], rightOperand,
			 * leftOperand); } else if (lflag == 1 && rflag == 1) { ret =
			 * equateColumns(columnFlag.get(rightOperand)[1], rightOperand,
			 * leftOperand, "like"); }
			 */
			break;
		}
		return ret;
	}

	private static HashSet<String> callNotEqualTo(String tableName,
			String columnName, String value) {

		HashSet<String> h1 = new HashSet<String>();
		HashSet<String> h2 = new HashSet<String>();
		String s1 = (String) BPlusTree.bPlusTreeStructure.get(tableName)
				.get(columnName).toString();
		String s2 = (String) BPlusTree.bPlusTreeStructure.get(tableName)
				.get(columnName).search(value);
		// System.out.println("@@@@:"+s1);

		if (s1 != "") {
			// System.out.println("%%%%" + s1);
			String tok[] = s1.split(",");
			for (String str : tok) {
				h1.add(str.trim());
			}
		}
		if (s2 != "") {
			String tok[] = s2.split(",");
			for (String str : tok) {
				h2.add(str.trim());
			}
		}
		// h2.retainAll(h1);
		h1.removeAll(h2);
		return h1;
	}

	private static HashSet<String> callRangeSearch(String tablename,
			String colname, String min, String max, boolean l, boolean h) {
		if (min == null)
			min = BPlusTree.bPlusTreeStructure.get(tablename).get(colname).min
					.toString();
		if (max == null)
			max = BPlusTree.bPlusTreeStructure.get(tablename).get(colname).max
					.toString();
		return BPlusTree.bPlusTreeStructure.get(tablename).get(colname)
				.rangeQuery(min, max, l, h);
	}

	private static HashSet<String> equateColumns(String tablename, String col1,
			String col2, String operator) {
		HashSet<String> h = new HashSet<String>();
		HashMap<String, String> h1 = BPlusTree.bPlusTreeStructure
				.get(tablename).get(col1).equateQuery();
		HashMap<String, String> h2 = BPlusTree.bPlusTreeStructure
				.get(tablename).get(col2).equateQuery();

		for (Entry<String, String> entry : h1.entrySet()) {
			if (h2.containsKey(entry.getKey())
					&& h2.get(entry.getKey()).equals(entry.getValue()))
				h.add(entry.getKey());
		}

		return h;
	}

	private static HashSet<String> callSearch(String tableName,
			String columnName, String value) {
		// System.out.println(":**********************************************************");
		// TODO Auto-generated method stub
		HashSet<String> h = new HashSet<String>();
		// System.out.println("Col:"+columnName+" value:"+value+"^");
		String s = (String) BPlusTree.bPlusTreeStructure.get(tableName)
				.get(columnName).search(value);
		// System.out.println(s);
		if (s != null) {
			String tok[] = s.split(",");
			for (String str : tok)
				h.add(str);
		}
		return h;
	}

	public static void initializeJoinVariables(TTableList tables) {

		if (tables.size() >= 2) {
			tableName1 = tables.getTable(0).getFullName();
			tableName2 = tables.getTable(1).getFullName();
			TSourceToken t = tables.getStartToken();
		}
	}

	static boolean checkValidity(String query) {
		sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		duplicateColOrNot.clear();
		sqlparser.sqltext = query;
		result = new StringBuilder();
		flag = 0;
		int ret = sqlparser.parse();
		if (ret == 0) {
			for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
				analyzeStmt(sqlparser.sqlstatements.get(i));
			}
			if (flag == 0) {
				// System.out.print(result);
				return true;
			} else {
				// System.out.print("Query Invalid");
				return false;
			}
		} else {
			// System.out.print("Query Invalid");
			return false;

		}

	}

	private static void analyzeStmt(TCustomSqlStatement stmt) {
		flag = 0;
		switch (stmt.sqlstatementtype) {
		case sstselect:
			analyzeSelectStmt((TSelectSqlStatement) stmt);
			break;
		case sstupdate:
			break;
		case sstcreatetable:
			analyzeCreateTableStmt((TCreateTableSqlStatement) stmt);
			break;
		case sstaltertable:
			break;
		case sstcreateview:
			break;
		default:
			System.out.println(stmt.sqlstatementtype.toString());
		}
	}

	protected static void analyzeCreateTableStmt(TCreateTableSqlStatement pStmt) {
		try {
			result.append("Querytype:create\n" + "Tablename:"
					+ pStmt.getTargetTable().toString() + "\nAttributes:");
			String comma = ",";
			String filecomma = ",";

			File file1 = new File(DBSystem.pathForData
					+ pStmt.getTargetTable().toString() + ".data");
			file1.createNewFile();
			File file2 = new File(DBSystem.pathForData
					+ pStmt.getTargetTable().toString() + ".csv");
			// file2.createNewFile();
			if (!file2.createNewFile()) {
				flag = 1;
				// System.out.println("File already exists.");
				return;
			}

			System.out.print(result);
			result.setLength(0);
			File readconfig = new File(configPath);

			FileWriter fw = new FileWriter(file1);
			BufferedWriter bw = new BufferedWriter(fw);

			FileWriter fwconfig = new FileWriter(readconfig, true);
			BufferedWriter bwconfig = new BufferedWriter(fwconfig);

			TColumnDefinition column;
			bwconfig.write("\nBEGIN\n" + pStmt.getTargetTable().toString()
					+ "\n");

			for (int i = 0; i < pStmt.getColumnList().size(); i++) {
				if (i == pStmt.getColumnList().size() - 1) {
					comma = "";
					filecomma = "";
				}
				column = pStmt.getColumnList().getColumn(i);
				System.out.print(column.getColumnName().toString() + " ");
				System.out.print(column.getDatatype().toString() + comma);

				bw.write(column.getColumnName().toString() + ":"
						+ column.getDatatype().toString() + filecomma);
				bwconfig.write(column.getColumnName().toString() + ", "
						+ column.getDatatype().toString() + "\n");
			}
			bwconfig.write("END");
			// bwconfig.write("END\n");
			bw.close();
			fw.close();
			bwconfig.close();
			fwconfig.close();
			DBSystem.pageTree.clear();
			DBSystem.readConfig(configPath);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected static void analyzeSelectStmt(TSelectSqlStatement pStmt) {

		invalidColumns = new LinkedList<String>();
		aliasTable = new HashMap<String, String>();

		result.append("Querytype:select\n");
		// System.out.println("Querytype:select");

		if (pStmt.isCombinedQuery()) {
			String setstr = "";
			switch (pStmt.getSetOperator()) {
			case 1:
				setstr = "union";
				break;
			case 2:
				setstr = "union all";
				break;
			case 3:
				setstr = "intersect";
				break;
			case 4:
				setstr = "intersect all";
				break;
			case 5:
				setstr = "minus";
				break;
			case 6:
				setstr = "minus all";
				break;
			case 7:
				setstr = "except";
				break;
			case 8:
				setstr = "except all";
				break;
			}
			System.out.printf("set type: %s\n", setstr);
			System.out.println("left select:");
			analyzeSelectStmt(pStmt.getLeftStmt());
			System.out.println("right select:");
			analyzeSelectStmt(pStmt.getRightStmt());
			if (pStmt.getOrderbyClause() != null) {
				System.out.printf("order by clause%s\n", pStmt
						.getOrderbyClause().toString());
			}
		} else {

			LinkedList<String> tables = new LinkedList<String>();
			String[] str = new String[2];
			columnFlag = new HashMap<String, String[]>();

			result.append("Tablename:");
			// System.out.print("Tablename:");
			String comma = ",";
			// System.out.println("++++++" + pStmt.joins.size());
			for (int i = 0; i < pStmt.joins.size(); i++) {
				if (i == pStmt.joins.size() - 1) {
					comma = "\n";
				}
				// System.out.println(pStmt.joins.size());
				TJoin join = pStmt.joins.getJoin(i);
				tables.add(join.getTable().toString().trim());
				switch (join.getKind()) {
				case TBaseType.join_source_fake:
					if (!DBSystem.pageTree.containsKey(join.getTable()
							.toString())) {
						flag = 1;
						return;
					}
					result.append(join.getTable().toString() + comma);

					if (join.getTable().getAliasClause() != null)
						aliasTable.put(join.getTable().toString(), join
								.getTable().getAliasClause().toString());

					// System.out.printf(">>>>>%s Alias: %s%s", join.getTable()
					// .toString(),
					// (join.getTable().getAliasClause() != null) ? join
					// .getTable().getAliasClause().toString()
					// : "", comma);
					// tables.add(join.getTable().toString());
					break;
				case TBaseType.join_source_table:
					if (!DBSystem.pageTree.containsKey(join.getTable()
							.toString())) {
						flag = 1;
						return;
					}
					result.append(join.getTable().toString() + comma);
					// System.out.printf("\ntable: \n\t%s, alias:%s\n", join
					// .getTable().toString(), (join.getTable()
					// .getAliasClause() != null) ? join.getTable()
					// .getAliasClause().toString() : "");

					for (int j = 0; j < join.getJoinItems().size(); j++) {
						TJoinItem joinItem = join.getJoinItems().getJoinItem(j);
						// System.out.printf("Join type:%s\n", joinItem
						// .getJoinType().toString());
						// System.out.printf("table: %s, alias:%s\n", joinItem
						// .getTable().toString(), (joinItem.getTable()
						// .getAliasClause() != null) ? joinItem
						// .getTable().getAliasClause().toString() : "");

						String table1 = join.getTable().toString().trim();
						String table2 = joinItem.getTable().toString().trim();

						tables.add(table2);
						if (joinItem.getOnCondition() != null) {

						} else if (joinItem.getUsingColumns() != null) {
						}
					}
					if (join.getTable().getAliasClause() != null)
						aliasTable.put(join.getTable().toString(), join
								.getTable().getAliasClause().toString());

					break;
				}
			}

			Iterator<String> it = tables.iterator();
			String tablename;
			Set<String> columnnames;

			while (it.hasNext()) {

				tablename = it.next();
				// System.out.println(tablename);
				columnnames = DBSystem.pageTree.get(tablename).coltypepair
						.keySet();
				for (String column : columnnames) {

					if (columnFlag.containsKey(column))
						invalidColumns.add(column);
					else {
						String newcol[] = new String[3];
						newcol[0] = DBSystem.pageTree.get(tablename).coltypepair
								.get(column);
						newcol[1] = tablename;
						newcol[2] = column;
						columnFlag.put(column, newcol);
					}
					String newcol[] = new String[3];
					newcol[0] = DBSystem.pageTree.get(tablename).coltypepair
							.get(column);
					newcol[1] = tablename;
					newcol[2] = column;
					columnFlag.put(tablename + "." + column, newcol);

					if (aliasTable.containsKey(tablename)) {
						newcol[0] = DBSystem.pageTree.get(tablename).coltypepair
								.get(column);
						newcol[1] = tablename;
						newcol[2] = column;
						columnFlag.put(
								aliasTable.get(tablename) + "." + column,
								newcol);
					}
				}
			}

			// removing invalid columns from linked list
			String col;
			it = invalidColumns.iterator();
			while (it.hasNext()) {
				col = it.next();
				if (columnFlag.containsKey(col))
					columnFlag.remove(col);
			}

			result.append("Columns:");
			comma = ",";

			for (int i = 0; i < pStmt.getResultColumnList().size(); i++) {
				if (i == pStmt.getResultColumnList().size() - 1)
					comma = "\n";
				TResultColumn resultColumn = pStmt.getResultColumnList()
						.getResultColumn(i);

				if (resultColumn.getExpr().toString().equalsIgnoreCase("*")) {
					it = tables.iterator();
					comma = ",";
					while (it.hasNext()) {
						tablename = it.next();
						columnnames = DBSystem.pageTree.get(tablename).coltypepair
								.keySet();
						int j = 0;
						for (String column : columnnames) {
							j++;
							if (it.hasNext() == false
									&& j == columnnames.size()) {
								comma = "\n";
							}
							// if (!columnFlag.containsKey(column))
							if (!duplicateColOrNot.containsKey(column)) {
								duplicateColOrNot.put(column, true);
								// column = tablename + "." + column;
								result.append(column + comma);
							}
						}
					}
					result.setCharAt(result.length() - 1, '\n');
				} else {
					String trimmedColumn = resultColumn.getExpr().toString();
					trimmedColumn = trimmedColumn.replace('(', ' ');
					trimmedColumn = trimmedColumn.replace(')', ' ');
					trimmedColumn = trimmedColumn.trim();

					if (!columnFlag.containsKey(trimmedColumn)) {

						// System.out.println("$$$"+trimmedColumn);
						flag = 1;
						return;
					}
					result.append(trimmedColumn + comma);
				}
			}
			result.append("Distinct:");

			if (pStmt.getSelectDistinct() != null) {
				String trimmedColumn = pStmt.getResultColumnList()
						.getResultColumn(0).toString();
				trimmedColumn = trimmedColumn.replace('(', ' ');
				trimmedColumn = trimmedColumn.replace(')', ' ');
				trimmedColumn = trimmedColumn.trim();
				result.append(trimmedColumn + "\n");
			} else {
				result.append("NA\n");
			}

			// where clause
			result.append("Condition:");
			// System.out.print("Condition:");
			if (pStmt.getWhereClause() != null) {

				validateWhereHaving(pStmt.getWhereClause().getCondition());
				if (flag == 1) {
					// System.out.println("Going Out!!!");
					return;
				}
				result.append(pStmt.getWhereClause().getCondition().toString()
						+ "\n");

			} else {
				result.append("NA\n");
			}

			// order by
			result.append("Orderby:");
			// System.out.printf("Orderby:");
			comma = ",";
			if (pStmt.getOrderbyClause() != null) {
				for (int i = 0; i < pStmt.getOrderbyClause().getItems().size(); i++) {
					if (i == pStmt.getOrderbyClause().getItems().size() - 1)
						comma = "\n";
					if (!columnFlag.containsKey(pStmt.getOrderbyClause()
							.getItems().getOrderByItem(i).toString())) {
						flag = 1;
						return;
					}
					result.append(pStmt.getOrderbyClause().getItems()
							.getOrderByItem(i).toString()
							+ comma);
					// System.out.printf("%s%s",
					// pStmt.getOrderbyClause().getItems().getOrderByItem(i).toString(),
					// comma);
				}
			} else {
				result.append("NA\n");
				// System.out.println("NA");
			}

			// group by
			result.append("Groupby:");
			// System.out.print("Groupby:");
			if (pStmt.getGroupByClause() != null) {
				String grpbytoks[] = pStmt.getGroupByClause().toString()
						.split(" ");

				String[] split = grpbytoks[2].split(",");
				for (String s : split) {
					if (!columnFlag.containsKey(s)) {
						flag = 1;

						return;
					}
				}
				result.append(grpbytoks[2] + "\n");

				// System.out.printf("%s\n", grpbytoks[2]);
			} else {
				result.append("NA\n");
				// System.out.println("NA");
			}
			result.append("Having:");
			if (pStmt.getGroupByClause() != null) {
				if (pStmt.getGroupByClause().getHavingClause() != null) {

					validateWhereHaving(pStmt.getGroupByClause()
							.getHavingClause());
					if (flag == 1) {
						return;
					}

					result.append(pStmt.getGroupByClause().getHavingClause()
							.toString());
					// System.out.println(pStmt.getGroupByClause().getHavingClause().toString());

				} else {
					result.append("NA");
					// System.out.println("NA");
				}
			} else {
				result.append("NA");
				// System.out.println("NA");
			}

			// for update
			if (pStmt.getForUpdateClause() != null) {
				System.out.printf("for update:\n%s\n", pStmt
						.getForUpdateClause().toString());
			}
			// top clause
			if (pStmt.getTopClause() != null) {
				System.out.printf("top clause:\n%s\n", pStmt.getTopClause()
						.toString());
			}
			// limit clause
			if (pStmt.getLimitClause() != null) {
				System.out.printf("top clause:\n%s\n", pStmt.getLimitClause()
						.toString());
			}
		}
	}

	private static void validateWhereHaving(TExpression condition) {
		// TODO Auto-generated method stub
		/*
		 * if ( flag == 1) return;
		 */
		/*
		 * try{ { if ( condition != null){ System.out.println("$$$"+condition);
		 * //System.out.println("$$"+condition.getLeftOperand()
		 * +"$$"+condition.getOperatorToken
		 * ()+"$$"+condition.getRightOperand()+"$$");
		 * 
		 * if ( condition.getRightOperand() != null) {
		 * System.out.println("going right");
		 * validateWhereHaving(condition.getRightOperand()); } if (
		 * condition.getLeftOperand() != null) {
		 * System.out.println("going left");
		 * validateWhereHaving(condition.getLeftOperand()); } } } } catch
		 * (Exception e){}
		 */

		if (condition != null && condition.getLeftOperand() == null
				&& condition.getRightOperand() == null) {
			// System.out.println("$$$" + condition);
			String cond = condition.toString();
			if (columnFlag.containsKey(cond))
				return;
			else {
				try {
					Integer.parseInt(cond);
					return;
				} catch (NumberFormatException e) {
					// System.out.println(leftOperand+" is not a integer");
					try {
						Float.parseFloat(cond);
						return;
					} catch (NumberFormatException e1) {
						// System.out.println(leftOperand+" is not a float");
						char leftarray[] = cond.toCharArray();
						if (leftarray[0] == '\''
						// *****************************length -1 missing
								&& leftarray[leftarray.length - 1] == '\'') {
							return;
						} else {
							flag = 1;
							return;
						}
					}
				}
			}
		}
		if (condition.getOperatorToken() == null
				|| condition.getOperatorToken().toString()
						.equalsIgnoreCase("and")
				|| condition.getOperatorToken().toString()
						.equalsIgnoreCase("or")) {
			if (condition.getRightOperand() != null)
				validateWhereHaving(condition.getRightOperand());
			if (condition.getLeftOperand() != null)
				validateWhereHaving(condition.getLeftOperand());
		} else {

			if (condition.getLeftOperand() != null
					&& condition.getRightOperand() != null) {
				// System.out.println("$$"+condition.getLeftOperand()
				// +"$$"+condition.getOperatorToken()+"$$"+condition.getRightOperand()+"$$");
				verifyOperandType(condition);

			}
		}
	}

	private static void fillJoinAttributes(TExpression condition,
			String table1, String table2,
			HashMap<String, HashSet<String>> joinPair) {

		// System.out.println("$$$" + condition);

		if (condition != null && condition.getLeftOperand() == null
				&& condition.getRightOperand() == null) {
			return;
		}
		if (condition.getOperatorToken() == null
				|| condition.getOperatorToken().toString()
						.equalsIgnoreCase("and")
				|| condition.getOperatorToken().toString()
						.equalsIgnoreCase("or")) {

			// System.out.println("Condition:"+condition+":left:"+condition.getLeftOperand()+":Operator:"+condition.getOperatorToken()+":Right:"+condition.getRightOperand());

			if (condition.getLeftOperand() != null) {
				fillJoinAttributes(condition.getLeftOperand(), table1, table2,
						joinPair);
				// System.out.println("Getting:"+h2+":condition:"+condition);
			}
			if (condition.getRightOperand() != null) {
				fillJoinAttributes(condition.getRightOperand(), table1, table2,
						joinPair);
				// System.out.println("Getting:"+h1+":condition:"+condition);
			}

			if (condition.getOperatorToken() == null) {
				// System.out.println("$$$" + condition);
				return;
			}

			if (condition.getOperatorToken().toString().equalsIgnoreCase("and")) {
				return;
			}
			if (condition.getOperatorToken().toString().equalsIgnoreCase("or")) {
				return;
			}
		} else {

			if (condition.getLeftOperand() != null
					&& condition.getRightOperand() != null) {
				// System.out.println("$$" + condition.getLeftOperand() + "$$"
				// + condition.getOperatorToken() + "$$"
				// + condition.getRightOperand() + "$$");
				String leftCol = condition.getLeftOperand().toString().trim();
				String rightCol = condition.getRightOperand().toString().trim();
				// System.out.println("**" + leftCol);
				// System.out.println("&&" + rightCol);
				String leftTable = columnFlag.get(leftCol)[1];
				String rightTable = columnFlag.get(rightCol)[1];

				leftCol = leftTable + "." + columnFlag.get(leftCol)[2];
				rightCol = rightTable + "." + columnFlag.get(rightCol)[2];

				// fillJoinTableDetails(leftTable, leftCol, rightCol);
				// fillJoinTableDetails(rightTable, rightCol, leftCol);

				if (!joinPair.containsKey(leftCol)) {
					HashSet<String> hs = new HashSet<String>();
					joinPair.put(leftCol, hs);
				}
				joinPair.get(leftCol).add(rightCol);

				if (!joinPair.containsKey(rightCol)) {
					HashSet<String> hs = new HashSet<String>();
					joinPair.put(rightCol, hs);
				}
				joinPair.get(rightCol).add(leftCol);

				return;

			} else {
				return;
			}
		}
		return;
	}

	private static void verifyOperandType(TExpression condition) {
		String leftOperand = condition.getLeftOperand().toString();
		String rightOperand = condition.getRightOperand().toString();

		String dataTypeLeft = null;
		String dataTypeRight = null;

		// System.out.println("Left$$:"+leftOperand+" Right$$:"+rightOperand+" Operator$$:"+condition.getOperatorToken().toString());

		if (condition.getOperatorToken().toString().equalsIgnoreCase("like")) {

			if (rightOperand.charAt(0) != '\''
					|| rightOperand.charAt(rightOperand.length() - 1) != '\''
					|| !(columnFlag.containsKey(leftOperand))) {
				flag = 1;
				return;
			}
			dataTypeLeft = columnFlag.get(leftOperand)[0];
			dataTypeLeft = dataTypeLeft.trim();
			String chk = new String(dataTypeLeft.toCharArray(), 0, 3);
			if (!chk.equalsIgnoreCase("var"))
				flag = 1;

		}
		// System.out.println(leftOperand);
		else {
			dataTypeLeft = extractDataType(leftOperand);
			dataTypeRight = extractDataType(rightOperand);
			// System.out.println("DataType Left:"+dataTypeLeft+" DataType Right:"+dataTypeRight);
			if (!(dataTypeLeft.equalsIgnoreCase(dataTypeRight))) {
				flag = 1;
			}
		}
	}

}
