package com.db.phase2;

import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Vector;

public class SortMergeJoin {

	public static Vector<String> availableMemory = null;
	public static LinkedList<String> availableMemoryForJoin = null;
	public static String finalFileName = "Join-Output";
	public static Integer totalSublistForTable1;
	public static Integer totalSublistForTable2;
	public static Integer indexForTable1;
	public static Integer indexForTable2;
	public static String colTypeForTable1;
	public static String colTypeForTable2;
	public static BufferedWriter bufferedJoinOutputWriter;

	public static void delete(File file) throws IOException {

		if (file.isDirectory()) {
			// directory is empty, then delete it
			if (file.list().length == 0) {
				file.delete();
			} else {
				// list all the directory contents
				String files[] = file.list();
				for (String temp : files) {
					// construct the file structure
					File fileDelete = new File(file, temp);
					// recursive delete
					delete(fileDelete);
				}

				// check the directory again, if empty then delete it
				if (file.list().length == 0) {
					file.delete();
				}
			}
		} else {
			file.delete();
		}
	}

	public static int compare(String s1, String s2, String coltype) {
		// System.out.println("^^^^^^^" + s1);
		// System.out.println("%%%%%%%" + s2);
		if (coltype.length() >= 7
				&& coltype.substring(0, 7).equalsIgnoreCase("varchar")) {
			int result = s1.compareTo(s2);
			if (result == 0)
				return 1;
			else
				return result;
			// return (s1.compareTo(s2));
		} else {
			// System.out.println("Type:" + coltype + " Val1:" + s1 + " Val2:" +
			// s2);
			double l1 = Double.parseDouble(s1);
			double l2 = Double.parseDouble(s2);
			if (l1 > l2)
				return 1;
			else if (l1 < l2)
				return -1;
			else
				return 1;
		}
	}

	static int compareRecords(Integer colIndex, String s1, String s2,
			String colType) {

		// System.out.println("--" + s1);
		// System.out.println("--" + s2);
		String split1[] = DBSystem.splitTableRow(s1);
		String split2[] = DBSystem.splitTableRow(s2);
		String comp1, comp2;

		int result = 0;
		comp1 = split1[colIndex].trim();
		comp2 = split2[colIndex].trim();

		result = compare(comp1, comp2, colType);
		if (result != 0) {
			return result;
		}

		return -1;
	}

	public static int compareForJoin(String s1, String s2, String coltype) {
		// System.out.println("^^^^^^^" + s1);
		// System.out.println("%%%%%%%" + s2);
		if (coltype.length() >= 7
				&& coltype.substring(0, 7).equalsIgnoreCase("varchar")) {
			int result = s1.compareTo(s2);
			return result;
			// return (s1.compareTo(s2));
		} else {
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

	static int compareRecordsForJoin(Integer indexForTable1,
			Integer indexForTable2, String s1, String s2,
			String colTypeForTable1, String colTypeForTable2) {

		// System.out.println("--" + s1);
		// System.out.println("--" + s2);
		String split1[] = DBSystem.splitTableRow(s1);
		String split2[] = DBSystem.splitTableRow(s2);
		String comp1, comp2;

		int result = 0;
		comp1 = split1[indexForTable1].trim();
		comp2 = split2[indexForTable2].trim();

		result = compareForJoin(comp1, comp2, colTypeForTable1);
		return result;
	}

	public static void sortVector(final String tableName,
			Vector<String> recordVector, final Integer colIndex,
			final String colType) {
		try {

			Comparator<String> cmp = new Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					// TODO Auto-generated method stub
					return (compareRecords(colIndex, s1, s2, colType));
				}

			};
			Collections.sort(recordVector, cmp);

			// for (int i = 0; i < recordVector.size(); i++) {
			// System.out.println(recordVector.get(i));
			// }

		} catch (Exception e) {

		}
	}

	public static Integer createSortedSublist(String tablePath,
			String tableName, Integer colIndex, String colType,
			Integer maxSubList) {
		Integer subListCount = 0;
		try {
			availableMemory = new Vector<String>(maxSubList + 1);
			String subListPath = tablePath + "/" + tableName + "-sublist";
			String tableFilePath = tablePath + "/" + tableName + ".csv";
			File subListFolder = new File(subListPath);
			if (subListFolder.exists())
				delete(subListFolder);

			subListFolder.mkdir();

			File tableFilePointer = new File(tableFilePath);
			BufferedReader tableReader = new BufferedReader(new FileReader(
					tableFilePointer));
			int totalRecords = 0;

			String line;
			while ((line = tableReader.readLine()) != null) {
				totalRecords++;
				if (totalRecords == maxSubList) {
					availableMemory.add(line);
					// sort
					sortVector(tableName, availableMemory, colIndex, colType);
					// dump
					File newFile = new File(subListFolder + "/sublist"
							+ subListCount);
					newFile.createNewFile();
					BufferedWriter bW = new BufferedWriter(new FileWriter(
							newFile));
					for (String s : availableMemory) {
						bW.write(s + "\n");
					}
					subListCount++;
					totalRecords = 0;
					bW.close();
					availableMemory.clear();
				} else {
					// put
					availableMemory.add(line);
				}
			}
			if (totalRecords > 0) {
				// sort
				sortVector(tableName, availableMemory, colIndex, colType);
				// dump
				File newFile = new File(subListFolder + "/sublist"
						+ subListCount);
				newFile.createNewFile();
				BufferedWriter bW = new BufferedWriter(new FileWriter(newFile));
				for (String s : availableMemory) {
					bW.write(s + "\n");
				}
				subListCount++;
				totalRecords = 0;
				bW.close();
			}

			tableReader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return subListCount;

	}

	public static void createSublistsForTables(String tablePath, String table1,
			String colName1, String table2, String colName2, Integer maxSubLists) {
		String colType1 = DBSystem.pageTree.get(table1).coltypepair.get(
				colName1).trim();
		String colType2 = DBSystem.pageTree.get(table2).coltypepair.get(
				colName2).trim();
		int index = 0;

		for (Entry<String, String> entry : DBSystem.pageTree.get(table1).coltypepair
				.entrySet()) {
			String colName = entry.getKey().trim();
			if (colName.equals(colName1))
				break;
			else
				index++;

		}
		indexForTable1 = index;
		colTypeForTable1 = colType1;
		totalSublistForTable1 = createSortedSublist(tablePath, table1, index,
				colType1, maxSubLists);

		index = 0;

		for (Entry<String, String> entry : DBSystem.pageTree.get(table2).coltypepair
				.entrySet()) {
			String colName = entry.getKey().trim();
			if (colName.equals(colName2))
				break;
			else
				index++;

		}
		indexForTable2 = index;
		colTypeForTable2 = colType2;
		totalSublistForTable2 = createSortedSublist(tablePath, table2, index,
				colType2, maxSubLists);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void createMainlists(String tablePath, String table,
			Integer maxSubList, String colType, Integer index,
			Integer totalSubList) {
		try {
			String subListPath = tablePath + "/" + table + "-sublist";
			while (true) {
				String subListFileName[] = new String[maxSubList];
				File file = new File(subListPath);
				int i = 0;
				// System.out.println("Now Merging SulistFile:");
				File[] files = file.listFiles();

				Arrays.sort(files, new Comparator() {

					@Override
					public int compare(Object o1, Object o2) {
						if (((File) o1).lastModified() > ((File) o2)
								.lastModified()) {
							return -1;
						} else if (((File) o1).lastModified() < ((File) o2)
								.lastModified()) {
							return +1;
						} else {
							return 0;
						}
					}

				});

				for (File sublist : files) {
					subListFileName[i] = sublist.getName();

					// System.out.print("-->" + sublist.getName());
					i++;
					if (i == maxSubList) {
						i = 0;
						String finalFile = "sublist" + totalSubList;
						// for (String s : subListFileName) {
						// System.out.print(s + " ");
						// }
						// System.out.println();
						// System.out.println("Final File Name:" + finalFile);
						ExternalMergeSort ext = new ExternalMergeSort(
								maxSubList, maxSubList);
						ext.initialiseMergeJoin(subListFileName, subListPath,
								finalFile, index, colType);
						totalSubList++;
						break;
					}
				}
				// System.out.println(i);
				if (i > 0 && i < maxSubList) {
					//
					if (i == 1)
						break;
					else {
						String finalFile = "sublist" + totalSubList;
						ExternalMergeSort ext = new ExternalMergeSort(i, i);
						ext.initialiseMergeJoin(subListFileName, subListPath,
								finalFile, index, colType);
						totalSubList++;
						// System.out.println("Remaining------>");
						// for (int j = 0; j < i; j++) {
						// System.out.print(subListFileName[j] + " ");
						// }
						// System.out.println();
						// System.out.println("Final File Name:" + finalFile);
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createMainListForTables(String tablePath,
			Integer maxSubList, String table1, String table2) {
		createMainlists(tablePath, table2, maxSubList, colTypeForTable2,
				indexForTable2, totalSublistForTable2);
		createMainlists(tablePath, table1, maxSubList, colTypeForTable1,
				indexForTable1, totalSublistForTable1);
	}

	private static int processJoin(LinkedList<String> availableMemoryForJoin) {
		Integer listSize = availableMemoryForJoin.size() - 1;
		String recordFromTable1 = null;
		int i = 0;
		int sofar = 0;

		LinkedList<Integer> temp = new LinkedList<Integer>();

		for (String s : availableMemoryForJoin) {
			if (i == 0) {
				recordFromTable1 = s;
			} else {
				int ret = compareRecordsForJoin(indexForTable1, indexForTable2,
						recordFromTable1, s, colTypeForTable1, colTypeForTable2);
				if (ret < 0) {
					return 0;
				} else {
					if (ret == 0) {
						// System.out.println(recordFromTable1 + "," + s);
						try {
							bufferedJoinOutputWriter.write(recordFromTable1
									+ "," + s + "\n");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				}

			}
			i++;
		}

		return listSize;
	}

	private static boolean computeJoinAndDump(
			LinkedList<String> availableMemoryList,
			BufferedReader table2Reader, Integer maxSubList) {
		try {
			String lineFromTable2;
			int toFill = maxSubList - 1;
			int totalRecords = 0;
			while ((lineFromTable2 = table2Reader.readLine()) != null) {
				totalRecords++;
				availableMemoryForJoin.add(lineFromTable2);
				// System.out.println(lineFromTable2);
				// System.out.println(":"+totalRecords);
				if (totalRecords == toFill) {
					// System.out.println("I am in if");
					toFill = processJoin(availableMemoryForJoin);
					String line1 = availableMemoryForJoin.get(0);
					availableMemoryForJoin.clear();

					totalRecords = 0;
					if (toFill == 0) {
						return true;
					} else {
						availableMemoryForJoin.add(line1);
					}
				}
			}

			toFill = processJoin(availableMemoryForJoin);
			availableMemoryForJoin.clear();
			if (toFill == 0)
				return false;
			else
				return true;

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void loopJoin(Integer maxSubList,
			String tablePath, String table1, Integer indexForTable12,
			String table2, Integer indexForTable2) {
		try {

			File joinFilePointer = new File(tablePath + "/" + finalFileName);
			joinFilePointer.createNewFile();
			bufferedJoinOutputWriter = new BufferedWriter(new FileWriter(
					joinFilePointer));

			File f = new File(tablePath + "/" + table1 + "-sublist");
			File fileName[] = f.listFiles();
			BufferedReader table1Reader = new BufferedReader(new FileReader(
					fileName[0]));
			f = new File(tablePath + "/" + table2 + "-sublist");
			fileName = f.listFiles();
			BufferedReader table2Reader = new BufferedReader(new FileReader(
					fileName[0]));

			String lineFromTable1;
			availableMemoryForJoin = new LinkedList<String>();

			while ((lineFromTable1 = table1Reader.readLine()) != null) {
				availableMemoryForJoin.addFirst(lineFromTable1);
				// System.out.println("-->:" + lineFromTable1);
				computeJoinAndDump(availableMemoryForJoin, table2Reader,
						maxSubList);
				table2Reader.close();
				table2Reader = new BufferedReader(new FileReader(fileName[0]));
			}

			table1Reader.close();
			table2Reader.close();
			bufferedJoinOutputWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void displayResults(TSelectSqlStatement pStmt,
			String finalfile, String leftTable, String rightTable) {
		try {
			int h = 0;
			HashMap<String, Integer> hmp = new HashMap<String, Integer>();
			Iterator<Entry<String, String>> itr = DBSystem.pageTree
					.get(leftTable).coltypepair.entrySet().iterator();
			while (itr.hasNext()) {
				hmp.put(leftTable + "." + itr.next().getKey(), h++);
			}
			itr = DBSystem.pageTree.get(rightTable).coltypepair.entrySet()
					.iterator();
			while (itr.hasNext()) {
				hmp.put(rightTable + "." + itr.next().getKey(), h++);
			}
			FileReader fr = new FileReader(DBSystem.pathForData + "/"
					+ finalfile);
			BufferedReader br = new BufferedReader(fr);
			String line;
			TResultColumn resultColumn = pStmt.getResultColumnList()
					.getResultColumn(0);
			if (resultColumn.getExpr().toString().equalsIgnoreCase("*")) {
				int length = 0;
				Set<String> columnnames = DBSystem.pageTree.get(leftTable).coltypepair
						.keySet();

				for (String column : columnnames) {
					System.out.print("\"" + leftTable + "." + column + "\",");
				}
				columnnames = DBSystem.pageTree.get(rightTable).coltypepair
						.keySet();
				length = 0;
				for (String column : columnnames) {
					if (length < columnnames.size() - 1) {
						System.out.print("\"" + rightTable + "." + column
								+ "\",");
					} else {
						System.out.println("\"" + rightTable + "." + column
								+ "\"");
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
					String temp = pStmt.getResultColumnList()
							.getResultColumn(i).toString().trim();
					String tableName = Main.columnFlag.get(temp)[1];
					String actualColName = Main.columnFlag.get(temp)[2];
					String finalColName = (tableName + "." + actualColName);
					System.out.print("\"" + finalColName + "\",");
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
						String tableName = Main.columnFlag.get(resultColumn
								.toString().trim())[1];
						String actualColName = Main.columnFlag.get(resultColumn
								.toString().trim())[2];
						String finalColName = (tableName + "." + actualColName);
						System.out.print("\"" + l[hmp.get(finalColName)]
								+ "\",");
					}
					resultColumn = pStmt.getResultColumnList().getResultColumn(
							i);
					String tableName = Main.columnFlag.get(resultColumn
							.toString().trim())[1];
					String actualColName = Main.columnFlag.get(resultColumn
							.toString().trim())[2];
					String finalColName = (tableName + "." + actualColName);

					System.out.println("\"" + l[hmp.get(finalColName)] + "\"");
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

	public static void sortMergeMain(String tablePath, String table1,
			String colName1, String table2, String colName2) {
		Integer maxSubList = 400000;
		// System.out.println("tablePath:" + tablePath + ":table1:" + table1
		// + ":colName1:" + colName1 + ":table2:" + table2 + ":colName2:"
		// + colName2);

		createSublistsForTables(tablePath, table1, colName1, table2, colName2,
				maxSubList);
		createMainListForTables(tablePath, maxSubList, table1, table2);

		loopJoin(maxSubList, tablePath, table1, indexForTable1,
				table2, indexForTable2);
		// System.out.println(colTypeForTable1 + "--" + indexForTable2 + "--"
		// + totalSublistForTable2);

	}
}
