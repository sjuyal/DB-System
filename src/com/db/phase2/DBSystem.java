package com.db.phase2;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import com.db.phase2.DBSystem.pageNumInfo;

public class DBSystem {

	static StringBuilder s;
	static int pageSize;
	static int pageCount;
	static String pathForData;
	static Integer temp;
	static LinkedHashMap<String, tableInfo> pageTree = new LinkedHashMap<String, tableInfo>();
	static RandomAccessFile file;
	static String ConfigFilePath = new String();

	public static class pageContent {
		int startingLine;
		int endingLine;
		int pageNumber;
		String tableName;
		// String data;
		Vector<String> data;
		int slotNo;
		int sizeOfData;
	}

	static pageContent pC, pC1;

	static LinkedHashMap<pageContent, Boolean> LRU = new LinkedHashMap<pageContent, Boolean>();

	public static class pageNumInfo {
		int startingLine;
		int endingLine;
		int startFileOffset;
		int size;
	}

	public static class tableInfo {
		int totalCols;
		int totalRecords;
		LinkedHashMap<String, String> coltypepair = new LinkedHashMap<String, String>();
		Vector<pageNumInfo> pageNum = new Vector<pageNumInfo>();
	}

	public static String[] splitTableRow(String row) {

		row = row.trim();
		String[] tempRow = null;
		if (row.charAt(0) == '"') {
			tempRow = row.split("\",\"");
			tempRow[0] = tempRow[0].substring(1);
			tempRow[tempRow.length - 1] = tempRow[tempRow.length - 1]
					.substring(0, tempRow[tempRow.length - 1].length() - 1);
		} else {
			tempRow = row.split(",");
		}
		for (int i = 0; i < tempRow.length; i++) {
			tempRow[i] = tempRow[i].trim();
		}
		return tempRow;
	}

	public static void printMyPageTable() {
		String key;
		for (Entry<String, tableInfo> entry : pageTree.entrySet()) {
			key = entry.getKey();
			System.out.println("This Is Table Name" + key);
			System.out.println("Its Info:");
			System.out.println("Total Records:"
					+ pageTree.get(key).totalRecords + ":Total Cols:"
					+ pageTree.get(key).totalCols);

			Iterator<pageNumInfo> itr = pageTree.get(key).pageNum.iterator();
			int pageNumber = 0;
			while (itr.hasNext()) {
				System.out.println("Page No:" + pageNumber
						+ "Record:Its End Line:" + itr.next().endingLine);
				pageNumber++;
			}

		}
	}

	public static Vector<String> insertInLRU(pageContent pC1, int flag) {
		// System.out.println("insertinLRU called--------------------------------------------------------------");
		pageContent pC;

		if (LRU.size() == pageCount)// if LRU is full
		{

			pC = LRU.keySet().iterator().next();
			LRU.remove(pC);
			pC1.slotNo = pC.slotNo;
			// System.out.println("Miss:Removing Page:"+pC.pageNumber);
		} else
			pC1.slotNo = LRU.size();

		LRU.put(pC1, true);
		// System.out.println("Miss:Adding Page to Queue..");
		// if (flag == 1)
		// System.out.println("MISS " + pC1.slotNo);
		return (pC1.data);
	}

	public static long V(String tableName, String columnName) {
		return BPlusTree.bPlusTreeStructure.get(tableName).get(columnName).distinctCols;
	}

	public static Vector<String> getLine(String tableName, int start, int end,
			int pageNumber, pageContent pC) throws IOException {// System.out.println("getLine called--------------------------------------------------------------");
		// s = new StringBuilder();
		file = new RandomAccessFile(pathForData + "/" + tableName + ".csv", "r");
		file.seek(pageTree.get(tableName).pageNum.get(pageNumber).startFileOffset);

		// String [] s = new String[];
		Vector<String> v = new Vector<String>();
		String s1;
		int length = 0;
		for (int i = start; i <= end; i++) { // s.append(file.readLine());
												// s.append("\n");
			s1 = file.readLine();
			if (s1 != null) {
				s1.trim();
				// if (s1.charAt(0) == '"') {
				// // s1 = s1.replace("\"", "").trim();
				// s1 = s1.replace("\",\"", ",");
				// s1 = s1.substring(1, s1.length() - 1);
				// s1 = s1.trim();
				// }

			}

			// s[i - start] = s1;
			v.add(s1);
			length += s1.length();
			// System.out.println("this is my record"+s);
		}
		pC.sizeOfData = length;
		file.close();
		// System.out.println("this is my record"+s);
		return v;
	}

	public static pageContent fetchRecords(String tableName, int recordId)
			throws IOException {
		pC = new pageContent();
		pC.tableName = tableName;
		String key = null;
		int end;
		int start;
		int mid;

		for (Entry<String, tableInfo> entry : pageTree.entrySet()) {
			key = entry.getKey();
			if (key.equals(tableName)) {
				start = 0;
				end = pageTree.get(key).pageNum.size() - 1;
				// binary search on vector
				while (start <= end) {
					mid = (start + end) / 2;
					if (recordId <= pageTree.get(key).pageNum.get(mid).endingLine
							&& recordId >= pageTree.get(key).pageNum.get(mid).startingLine) {
						pC.data = getLine(
								tableName,
								pageTree.get(key).pageNum.get(mid).startingLine,
								pageTree.get(key).pageNum.get(mid).endingLine,
								mid, pC);
						pC.endingLine = pageTree.get(key).pageNum.get(mid).endingLine;
						pC.startingLine = pageTree.get(key).pageNum.get(mid).startingLine;
						pC.pageNumber = mid;
						return pC;
					} else {
						if (recordId > pageTree.get(key).pageNum.get(mid).endingLine)
							start = mid + 1;
						else
							end = mid - 1;
					}
				}
				break;
			}
		}
		return null;
	}

	public static pageContent updateTableAndDisk(String tableName, String record)
			throws IOException {

		long fileOffset;

		// updating table entry
		int lastPage;
		lastPage = pageTree.get(tableName).pageNum.size() - 1;
		pageTree.get(tableName).totalRecords++;
		pC = new pageContent();

		if (pageTree.get(tableName).pageNum.get(lastPage).size
				+ record.length() <= pageSize)// add 1 if '\n' needed at
												// end-------------
		{
			pC = fetchRecords(tableName,
					pageTree.get(tableName).totalRecords - 2);
			pC.endingLine = ++pageTree.get(tableName).pageNum.get(lastPage).endingLine;

			// pC.data += record + "\n";
			// pC.data[pC.endingLine - pC.startingLine] = record;
			pC.data.add(record);
			pC.sizeOfData = pC.sizeOfData + record.length(); // doubt in this 1
			/*
			 * pageTree.get(tableName).pageNum.get(lastPage).size = pC.data
			 * .length();
			 */// size will contain extra '\n' count
			pageTree.get(tableName).pageNum.get(lastPage).size = pC.sizeOfData;

			file = new RandomAccessFile(pathForData + "/" + tableName + ".csv",
					"rw");
			fileOffset = file.length();
		} else {
			file = new RandomAccessFile(pathForData + "/" + tableName + ".csv",
					"rw");
			fileOffset = file.length();

			// pC.data = record + "\n";
			pC.data.add(record);
			pC.sizeOfData = record.length();// new included
			pC.endingLine = pageTree.get(tableName).pageNum.get(lastPage).endingLine + 1;
			pC.startingLine = pageTree.get(tableName).pageNum.get(lastPage).endingLine + 1;
			pC.pageNumber = lastPage + 1;
			pC.tableName = tableName;

			pageNumInfo p = new pageNumInfo();
			// p.size=record.length();
			// p.size = pC.data.length();
			p.size = pC.sizeOfData;
			p.endingLine = pC.endingLine;
			p.startFileOffset = (int) fileOffset;
			p.startingLine = pC.startingLine;
			pageTree.get(tableName).pageNum.add(lastPage + 1, p);
		}
		// insert in disk
		try {
			file.seek(fileOffset);
			file.writeBytes(record + "\n");
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return pC;
	}

	protected static void readConfig(String configFilePath) {

		// System.out.println("#######################################");

		File file = new File(configFilePath);
		FileInputStream fileConfig = null;
		char c;
		StringBuilder string;
		StringBuilder tableName;
		String tableNameString;
		int content;

		try {
			fileConfig = new FileInputStream(file);
			// reading page size
			string = new StringBuilder();
			while ((c = (char) (fileConfig.read())) != ' ')
				;
			while ((c = (char) (fileConfig.read())) != '\n')
				string.append(c);
			pageSize = Integer.parseInt(string.toString());
			// System.out.println(pageSize);

			// reading page count
			string = new StringBuilder();
			while ((c = (char) (fileConfig.read())) != ' ')
				;
			while ((c = (char) (fileConfig.read())) != '\n')
				string.append(c);
			pageCount = Integer.parseInt(string.toString());
			// System.out.println(pageCount);

			// reading pathForData
			string = new StringBuilder();
			while ((c = (char) (fileConfig.read())) != ' ')
				;
			int index = 0;
			while ((c = (char) (index = fileConfig.read())) != '\n'
					&& index != -1)
				string.append(c);
			pathForData = string.toString() + "/";

			// extracting tables
			while (true) {
				// searching for EOF
				if (fileConfig.read() == -1)
					break;

				// skipping "BEGIN"
				while ((c = (char) (fileConfig.read())) != '\n')
					;

				// reading table name
				tableName = new StringBuilder();
				while ((c = (char) (fileConfig.read())) != '\n')
					tableName.append(c);
				tableNameString = tableName.toString().trim();

				tableInfo inf = new tableInfo();// Initializing object for new
												// table
				inf.totalCols = 0;
				inf.totalRecords = 0;
				pageTree.put(tableNameString, inf);

				// System.out.println("this is table name:"+tableName);
				// extracting table fields and their type

				while (true) {
					// reading table column or end
					string = new StringBuilder();
					while ((c = (char) (content = fileConfig.read())) != ','
							&& c != '\n' && content != -1)
						string.append(c);
					// System.out.println("---------->"+string.toString());
					String columnExtracted = string.toString().trim();
					if (columnExtracted.length() == 11
							&& columnExtracted.substring(0, 11).equals(
									"PRIMARY_KEY")) {
						// System.out.println("===============");
						continue;
					}
					if (columnExtracted.equals("END"))
						break;

					temp = pageTree.get(tableNameString).totalCols;
					pageTree.get(tableNameString).totalCols++;// increasing
																// columns count

					// reading column type
					StringBuilder type = new StringBuilder();
					while ((c = (char) (fileConfig.read())) != '\n')
						type.append(c);
					// System.out.println("***:"+columnExtracted);
					pageTree.get(tableNameString).coltypepair.put(
							columnExtracted, type.toString().trim());
					// System.out.println(type.toString());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileConfig != null)
					fileConfig.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		// System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
	}

	public static void populateDBInfo() {

		File file;
		FileInputStream fileConfig;
		PrintStream fileOut = null;
		StringBuilder string;
		char c;
		String key;
		int content;
		int curBytes;
		int totalBytes;
		String record;
		int startingLine;
		int currentLine;
		int startFileOffset;
		int currentFileOffset;
		int tempOffset;
		pageNumInfo p;
		int content1 = 0;
		for (Entry<String, tableInfo> entry : pageTree.entrySet()) {
			key = entry.getKey();
			// System.out.println("My Current Key:"+key);
			file = new File(pathForData + "/" + key + ".csv");
			fileConfig = null;
			currentLine = -1;// line number starting from 0
			startingLine = 0;// starting from 0
			totalBytes = 0;// starting from 1
			curBytes = 0;// starting from 1
			startFileOffset = 0;// starting from 1
			currentFileOffset = -1;// starting from 1
			content1 = 0;
			// startingLine =0;
			// startFileOffset=0;

			try {
				fileConfig = new FileInputStream(file);
				while ((content = fileConfig.read()) != -1) {
					// this is current Line Number
					currentLine++;
					// fetching record id
					string = new StringBuilder();
					string.append((char) (content));
					tempOffset = ++currentFileOffset;

					while ((c = (char) (content1 = fileConfig.read())) != '\n'
							&& content1 != -1) {
						++currentFileOffset;
						string.append(c);
					}
					++currentFileOffset;// for '\n'

					record = string.toString();
					curBytes = record.length() + 1;
					// curBytes = record.getBytes().length;

					// checking limit
					if (curBytes + totalBytes - 1 > pageSize)// subtract -1 if
																// '\n'not
																// needed at
																// end-------------
					{
						p = new pageNumInfo();
						p.startingLine = startingLine;
						p.endingLine = currentLine - 1;
						p.size = totalBytes;
						p.startFileOffset = startFileOffset;
						pageTree.get(key).pageNum.add(p);
						totalBytes = curBytes;
						// startingLine=Integer.parseInt(record.split(",")[0]);
						startingLine = currentLine;
						startFileOffset = tempOffset;
					} else
						totalBytes += curBytes;
				}
				// System.out.println("Content1--------------------------------------------------:"+content1);

				p = new pageNumInfo();
				p.startingLine = startingLine;
				p.endingLine = currentLine;
				p.size = totalBytes;// size will include extra '\n' count
				p.startFileOffset = startFileOffset;
				pageTree.get(key).totalRecords = currentLine + 1;
				pageTree.get(key).pageNum.add(p);

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fileConfig != null)
						fileConfig.close();
					if (content1 == -1) {
						fileOut = new PrintStream(new FileOutputStream(file,
								true));
						;
						fileOut.append("\n");
						fileOut.close();
					}
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}

		// printMyPageTable();
	}

	public static String getRecord(String tableName, int recordId)
			throws IOException {
		// System.out.println("get record called--------------------------------------------------------------");
		// LRU Implemented Here
		// ------------------------------------------------------>
		// record id is line number
		Iterator<pageContent> i = LRU.keySet().iterator();
		while (i.hasNext()) {
			pC = i.next();
			if (pC.tableName.equals(tableName)) {
				if (recordId <= pC.endingLine && recordId >= pC.startingLine) {
					LRU.remove(pC);
					LRU.put(pC, true);
					// System.out.println("HIT");
					return (pC.data.get(recordId - pC.startingLine));
				}
			}
		}

		pC = fetchRecords(tableName, recordId);
		if (pC == null)
			return null;
		else {
			// System.out.println("above record Id:"+recordId+"pC value:"+pC.startingLine);
			// for(pageContent page : LRU)
			// System.out.println(page.pageNumber);
			// System.out.println("page number:"+pC.pageNumber);
			// System.out.println("$$"+(pC.data).split("\n")[recordId -
			// pC.startingLine]+"$$");
			// return insertInLRU(pC, 1).split("\n")[recordId -
			// pC.startingLine];
			// return insertInLRU(pC, 1)[recordId - pC.startingLine];
			return insertInLRU(pC, 1).get(recordId - pC.startingLine);
		}
	}

	public static void inserInIndex(String tableName, String record) {

		// insertRecord(tableName, record);
		String[] colval = DBSystem.splitTableRow(record);
		int i = 0;
		Vector<pageNumInfo> v = DBSystem.pageTree.get(tableName).pageNum;
		Integer lineNumber = v.get(v.size() - 1).endingLine;
		for (Entry<String, String> colentry : DBSystem.pageTree.get(tableName).coltypepair
				.entrySet()) {
			String colName = colentry.getKey();
			if (BPlusTree.bPlusTreeStructure.get(tableName)
					.containsKey(colName)) {
				BPlusTree.bPlusTreeStructure.get(tableName).get(colName)
						.addWithDuplicationHandled(colval[i], lineNumber);
			}
			i++;
		}

	}

	public static void insertRecord(String tableName, String record)

	throws IOException {
		int lastPage;
		Iterator<pageContent> i = LRU.keySet().iterator();
		lastPage = pageTree.get(tableName).pageNum.size() - 1;
		// System.out.println("insert record called-------------------------------------");
		while (i.hasNext()) {
			pC = i.next();
			if (pC.tableName == tableName && pC.pageNumber == lastPage) {
				// space available for upcoming record
				// if (pC.data.length() + record.length() <= pageSize) // add 1
				// if '\n'
				// needed at
				if (pC.sizeOfData + record.length() <= pageSize) // end-------------
				{
					i.remove();
					// pC.data += record+"\n";
					pC.data.add(record);
					LRU.put(pC, true);
					updateTableAndDisk(tableName, record);
					// System.out.println("Last Page Found and has enough space");
					return;
				}
				// no space available for upcoming record
				else {
					// i.remove();
					// LRU.addLast(pC);
					pC = updateTableAndDisk(tableName, record);
					insertInLRU(pC, 0);
					// System.out.println("Last Page Found but had no enough space");
					return;
				}
			}
		}
		// if page not found
		pC = updateTableAndDisk(tableName, record);
		insertInLRU(pC, 0);
		DBSystem.inserInIndex(tableName, record);

	}

	public static void printLRU() {
		System.out.println("------------LRU-----------------");
		Iterator<Entry<pageContent, Boolean>> it = DBSystem.LRU.entrySet()
				.iterator();
		while (it.hasNext()) {
			Iterator<Entry<pageContent, Boolean>> itold = it;
			Map.Entry pairs = (Map.Entry) it.next();
			pageContent pC = (pageContent) pairs.getKey();
			System.out.println(":TableName:" + pC.tableName);
			System.out.println("Starting Line:" + pC.startingLine
					+ ":endingLine:" + pC.endingLine);
		}
		System.out.println("------------End Of LRU-----------------");
	}
}