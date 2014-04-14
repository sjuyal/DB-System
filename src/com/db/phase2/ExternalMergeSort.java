package com.db.phase2;

import gudusoft.gsqlparser.nodes.TOrderByItemList;

import java.io.*;
import java.util.*;

public class ExternalMergeSort {

	int totalFiles;
	int fragments;

	public ExternalMergeSort(int totalFiles, int fragments) {
		this.totalFiles = totalFiles;
		this.fragments = fragments;
	}

	public void initialiseMerge(String folderPath, String filename,
			String finalfile, String tableName,
			TOrderByItemList tOrderByItemList, HashMap<String, Integer> hmp) {
		int i, j;
		String[] dataLines = new String[fragments];
		// Integer[] hash = new Integer[fragments];
		String min, temp;
		int result;
		int minIndex = 0;
		Iterator<Integer> itr;
		Set<Integer> set = new LinkedHashSet<Integer>();
		FileReader[] file = new FileReader[fragments];
		File[] del = new File[fragments];
		// System.out.println("-------------"+fragments);
		try {
			FileWriter fd = new FileWriter(folderPath + "/" + finalfile);
			BufferedWriter bd = new BufferedWriter(fd);
			BufferedReader[] reader = new BufferedReader[totalFiles];
			for (i = 0; i < fragments; i++) {
				set.add(i);
				file[i] = new FileReader(folderPath + "/" + filename
						+ Integer.toString(i));
				del[i] = new File(folderPath + "/" + filename
						+ Integer.toString(i));
				reader[i] = new BufferedReader(file[i]);
				dataLines[i] = reader[i].readLine();
			}
			// find minimum among all of them
			while (!set.isEmpty()) {
				itr = set.iterator();
				i = itr.next();
				j = 0;
				while (dataLines[i].charAt(j) != ',')
					j++;

				// System.out.println("below 1while");
				min = dataLines[i];
				minIndex = i;
				// System.out.println("above while");
				while (itr.hasNext()) {
					i = itr.next();
					temp = dataLines[i];

					result = Main.compareRecords(tableName, min, temp,
							tOrderByItemList, hmp);
					if (result > 0) {
						minIndex = i;
						min = dataLines[i];
					}
				}
				// System.out.println("min:"+dataLines[minIndex]);
				bd.write(dataLines[minIndex] + "\n");
				// System.out.println("Gonna Write:"+dataLines[minIndex]+"\n");
				dataLines[minIndex] = reader[minIndex].readLine();
				if (dataLines[minIndex] == null) {
					set.remove(minIndex);
					del[minIndex].delete();
				}
			}
			bd.close();
			fd.close();
			// for(i = 0; i < fragments;i++)
			// reader[minIndex].close();
		} catch (Exception e) {
			System.out.println("Except" + e);
		}
	}

	public void initialiseMergeJoin(String[] sublistFileNamePath,
			String folderPath, String finalfile, int colIndex, String colType) {
		int i, j;
		String[] dataLines = new String[fragments];
		// Integer[] hash = new Integer[fragments];
		String min, temp;
		int result;
		int minIndex = 0;
		Iterator<Integer> itr;
		Set<Integer> set = new LinkedHashSet<Integer>();
		FileReader[] file = new FileReader[fragments];
		File[] del = new File[fragments];
		// System.out.println("-------------"+fragments);
		try {
			FileWriter fd = new FileWriter(folderPath + "/" + finalfile);
			BufferedWriter bd = new BufferedWriter(fd);
			BufferedReader[] reader = new BufferedReader[totalFiles];
			for (i = 0; i < fragments; i++) {
				set.add(i);
				// file[i]=new FileReader( folderPath
				// +"/"+filename+Integer.toString(i));
				file[i] = new FileReader(folderPath + "/"
						+ sublistFileNamePath[i]);
				del[i] = new File(folderPath + "/" + sublistFileNamePath[i]);
				reader[i] = new BufferedReader(file[i]);
				dataLines[i] = reader[i].readLine();
			}
			// find minimum among all of them
			while (!set.isEmpty()) {
				itr = set.iterator();
				i = itr.next();
				j = 0;
				while (dataLines[i].charAt(j) != ',')
					j++;

				// System.out.println("below 1while");
				min = dataLines[i];
				minIndex = i;
				// System.out.println("above while");
				while (itr.hasNext()) {
					i = itr.next();
					temp = dataLines[i];

					result = SortMergeJoin.compareRecords(colIndex, min, temp,
							colType);
					if (result > 0) {
						minIndex = i;
						min = dataLines[i];
					}
				}
				// System.out.println("min:"+dataLines[minIndex]);
				bd.write(dataLines[minIndex] + "\n");
				// System.out.println("Gonna Write:"+dataLines[minIndex]+"\n");
				dataLines[minIndex] = reader[minIndex].readLine();
				if (dataLines[minIndex] == null) {
					set.remove(minIndex);
					del[minIndex].delete();
				}
			}
			bd.close();
			fd.close();
			// for(i = 0; i < fragments;i++)
			// reader[minIndex].close();
		} catch (Exception e) {
			System.out.println("Except" + e);
		}
	}

}
