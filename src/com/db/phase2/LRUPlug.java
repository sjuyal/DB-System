package com.db.phase2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import com.db.phase2.DBSystem.pageContent;

public class LRUPlug {

	static int firstInsertedLine;
	static String tableName;
	static int flag = 0;
	static Vector<String> stringPointerVector = new Vector<String>();

	/*
	 * public static boolean LRUSetIterator(String table, int
	 * startingLineNumber) { if (seizeInsert == true) return false; else {
	 * tableName = table; firstInsertedLine = startingLineNumber; // set
	 * iterator try { DBSystem.getRecord(tableName, startingLineNumber); } catch
	 * (IOException e) { // TODO Auto-generated catch block e.printStackTrace();
	 * } return true; } }
	 */

	public static void initializer() {
		flag = 0;
		stringPointerVector.clear();
	}

	public static Boolean IsRecordInsertionSafe() {
		pageContent pC = null;

		if (DBSystem.LRU.size() < DBSystem.pageCount)
			return true;

		Iterator<Entry<pageContent, Boolean>> it = DBSystem.LRU.entrySet()
				.iterator();
		pC = it.next().getKey();
		// System.out.println("Current PC Top:"+pC.endingLine+"--"+pC.startingLine+":firstInsertedLine:"+firstInsertedLine+":tableName:"+tableName);
		if (pC.tableName.equals(tableName)
				&& pC.startingLine <= firstInsertedLine
				&& firstInsertedLine <= pC.endingLine)
			return false;
		else
			return true;
	}

	public static Boolean consecutiveInserts(String tblnme, int nextLineNumber) {
		if (flag == 0) {
			tableName = tblnme;
			firstInsertedLine = nextLineNumber;
			flag = 1;
		}

		// put line Number in LRU
		if (IsRecordInsertionSafe()) {
			// System.out.println("True Returned-------------------");
			try {
				stringPointerVector.add(DBSystem.getRecord(tableName,nextLineNumber));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// System.out.println("True:tableName:"+tableName+":nextLineNumber:"+nextLineNumber);
			return true;
		} else {
			// System.out.println("False Returned-------------------");
			// System.out.println("False:tableName:"+tableName+":nextLineNumber:"+nextLineNumber);
			return false;
		}
	}

	public static Vector<String> getVectorOfRecords() {
		return stringPointerVector;
	}

	/*public static ArrayList<pageContent> getIterator() {
		// it will iterator of LRU from startingLineNumber
		Iterator<Entry<pageContent, Boolean>> it = DBSystem.LRU.entrySet()
				.iterator();
		ArrayList<pageContent> pcs = new ArrayList<pageContent>();
		while (it.hasNext()) {

			Iterator<Entry<pageContent, Boolean>> itold = it;
			Map.Entry pairs = (Map.Entry) it.next();
			pageContent pC = (pageContent) pairs.getKey();
			if (pC.tableName.equals(tableName)
					&& pC.startingLine <= firstInsertedLine
					&& firstInsertedLine <= pC.endingLine) {
				pcs.add(pC);
				// System.out.println("$$$$" + pC.startingLine + "--"
				// + pC.endingLine);
				break;
			}
			// it.remove(); // avoids a ConcurrentModificationException
		}
		while (it.hasNext()) {
			pageContent pC = it.next().getKey();
			// System.out.println("$$$$"+pC.startingLine+"--"+pC.endingLine);
			pcs.add(pC);
		}

		
		 * for (int i = 0; i < pcs.size(); i++) { System.out.println("$$$PCS:" +
		 * pcs.get(i).startingLine + ":EndingLine" + pcs.get(i).endingLine); }
		 
		return pcs;
	}*/
}