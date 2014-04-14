package com.db.phase2;

import gudusoft.gsqlparser.pp.processor.type.comm.DistinctKeyWordProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import com.db.phase2.DBSystem.tableInfo;

public class BPlusTree {
	class Node {
		public int mNumKeys = 0;
		public String[] mKeys = new String[2 * T - 1];
		public Object[] mObjects = new Object[2 * T - 1];
		public Node[] mChildNodes = new Node[2 * T];
		public boolean mIsLeafNode;
		public Node mNextNode;
	}

	Long distinctCols = (long) 0;
	String max = "";
	String min = "";

	public int compare(String s1, String s2) {
		// System.out.println("^^^^^^^" + s1);
		// System.out.println("%%%%%%%" + s2);
		if (columnType.length() >= 7
				&& columnType.substring(0, 7).equalsIgnoreCase("varchar"))
			return (s1.compareTo(s2));
		else {
			// System.out.println("Type:"+columnType+" Val1:"+s1+" Val2:"+s2);
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

	public static LinkedHashMap<String, LinkedHashMap<String, BPlusTree>> bPlusTreeStructure = new LinkedHashMap<String, LinkedHashMap<String, BPlusTree>>();

	public Node n;
	public int index;
	private Node mRootNode;
	private static final int T = 4;
	public String columnType = new String();

	public BPlusTree() {
		mRootNode = new Node();
		mRootNode.mIsLeafNode = true;
	}

	public void addWithDuplicationHandled(String colentry, Object object) {
		if (min == "")
			min = colentry;
		else if (compare(colentry, min) < 1)
			min = colentry;
		if (max == "")
			max = colentry;
		else if (compare(colentry, max) > 0)
			max = colentry;
		// System.out.println("colentry:"+colentry+"min:"+min+" max:"+max);

		if (search(colentry) != null) {
			// System.out.println("$$");
			// System.out.println("--->" + colentry);
			StringBuilder sb = new StringBuilder();
			sb.append(n.mObjects[index]);
			sb.append(",");
			sb.append(object);
			// n.mObjects[index] = sb.toString();
			// System.out.println("Object" + sb);
			add(colentry, object);
		} else {
			distinctCols++;
			add(colentry, object);
		}
		// System.out.println("---end---");
	}

	public void add(String colentry, Object object) {
		Node rootNode = mRootNode;
		if (rootNode.mNumKeys == (2 * T - 1)) {
			Node newRootNode = new Node();
			mRootNode = newRootNode;
			newRootNode.mIsLeafNode = false;
			mRootNode.mChildNodes[0] = rootNode;
			splitChildNode(newRootNode, 0, rootNode); // Split rootNode and move
														// its median (middle)
														// key up into
														// newRootNode.
			insertIntoNonFullNode(newRootNode, colentry, object); // Insert the
																	// key
																	// into the
																	// B-Tree
																	// with
																	// root
																	// newRootNode.
		} else {
			insertIntoNonFullNode(rootNode, colentry, object); // Insert the key
																// into
																// the B-Tree
																// with
																// root
																// rootNode.
		}
	}

	// Split the node, node, of a B-Tree into two nodes that contain T-1 (and T)
	// elements and move node's median key up to the parentNode.
	// This method will only be called if node is full; node is the i-th child
	// of parentNode.
	// All internal keys (elements) will have duplicates within the leaf nodes.
	void splitChildNode(Node parentNode, int i, Node node) {
		Node newNode = new Node();
		newNode.mIsLeafNode = node.mIsLeafNode;
		newNode.mNumKeys = T;
		for (int j = 0; j < T; j++) { // Copy the last T elements of node into
										// newNode. Keep the median key as
										// duplicate in the first key of
										// newNode.
			newNode.mKeys[j] = node.mKeys[j + T - 1];
			newNode.mObjects[j] = node.mObjects[j + T - 1];
		}
		if (!newNode.mIsLeafNode) {
			for (int j = 0; j < T + 1; j++) { // Copy the last T + 1 pointers of
												// node into newNode.
				newNode.mChildNodes[j] = node.mChildNodes[j + T - 1];
			}
			for (int j = T; j <= node.mNumKeys; j++) {
				node.mChildNodes[j] = null;
			}
		} else {
			// Manage the linked list that is used e.g. for doing fast range
			// queries.
			newNode.mNextNode = node.mNextNode;
			node.mNextNode = newNode;
		}
		for (int j = T - 1; j < node.mNumKeys; j++) {
			node.mKeys[j] = null;
			node.mObjects[j] = null;
		}
		node.mNumKeys = T - 1;

		// Insert a (child) pointer to node newNode into the parentNode, moving
		// other keys and pointers as necessary.
		for (int j = parentNode.mNumKeys; j >= i + 1; j--) {
			parentNode.mChildNodes[j + 1] = parentNode.mChildNodes[j];
		}
		parentNode.mChildNodes[i + 1] = newNode;
		for (int j = parentNode.mNumKeys - 1; j >= i; j--) {
			parentNode.mKeys[j + 1] = parentNode.mKeys[j];
			parentNode.mObjects[j + 1] = parentNode.mObjects[j];
		}
		parentNode.mKeys[i] = newNode.mKeys[0];
		parentNode.mObjects[i] = newNode.mObjects[0];
		parentNode.mNumKeys++;
	}

	// Insert an element into a B-Tree. (The element will ultimately be inserted
	// into a leaf node).
	void insertIntoNonFullNode(Node node, String colentry, Object object) {
		int i = node.mNumKeys - 1;
		if (node.mIsLeafNode) {
			// Since node is not a full node insert the new element into its
			// proper place within node.
			while (i >= 0 && compare(colentry, node.mKeys[i]) < 0) {
				node.mKeys[i + 1] = node.mKeys[i];
				node.mObjects[i + 1] = node.mObjects[i];
				i--;
			}
			i++;
			node.mKeys[i] = colentry;
			node.mObjects[i] = object;
			node.mNumKeys++;
		} else {
			// Move back from the last key of node until we find the child
			// pointer to the node
			// that is the root node of the subtree where the new element should
			// be placed.
			while (i >= 0 && compare(colentry, node.mKeys[i]) < 0) {
				i--;
			}
			i++;
			if (node.mChildNodes[i].mNumKeys == (2 * T - 1)) {
				splitChildNode(node, i, node.mChildNodes[i]);
				if (compare(colentry, node.mKeys[i]) > 0) {
					i++;
				}
			}
			insertIntoNonFullNode(node.mChildNodes[i], colentry, object);
		}
	}

	// Recursive search method.
	public Object search(Node node, String colentry) {
		int i = 0;
		// System.out.println("Comp:"+compare(colentry, node.mKeys[0]));

		while (i < node.mNumKeys
				&& compare(colentry.trim(), node.mKeys[i].trim()) > 0) {
			i++;
		}
		if (i < node.mNumKeys && compare(colentry, node.mKeys[i]) == 0) {
			n = node;
			index = i;
			return node.mObjects[i];
		}
		if (node.mIsLeafNode) {
			return null;
		} else {
			return search(node.mChildNodes[i], colentry);
		}
	}

	public Object search(String colentry) {
		return search(mRootNode, colentry);
	}

	public HashMap<String, String> equateQuery() {

		HashMap<String, String> hsmap = new HashMap<String, String>();
		Node node = mRootNode;
		while (!node.mIsLeafNode) {
			node = node.mChildNodes[0];
		}
		while (node != null) {
			for (int i = 0; i < node.mNumKeys; i++) {
				hsmap.put(node.mObjects[i].toString(), node.mKeys[i]);
			}
			node = node.mNextNode;
		}
		return hsmap;
	}

	// Iterative search method.

	public HashSet<String> rangeQuery(String low, String high, boolean l,
			boolean h) {
		LinkedHashSet<String> hset = new LinkedHashSet<String>();
		Node node = mRootNode;
		while (!node.mIsLeafNode) {
			node = node.mChildNodes[0];
		}
		if (l && h) {
			while (node != null) {
				for (int i = 0; i < node.mNumKeys; i++) {
					if (compare(node.mKeys[i], high) <= 0
							&& compare(node.mKeys[i], low) >= 0) {
						String splt[] = DBSystem.splitTableRow(node.mObjects[i]
								.toString());
						for (String s : splt) {
							hset.add(s);
						}
					}
				}
				node = node.mNextNode;
			}
		} else if (!l && h) {
			while (node != null) {
				for (int i = 0; i < node.mNumKeys; i++) {
					if (compare(node.mKeys[i], high) <= 0
							&& compare(node.mKeys[i], low) > 0) {

						String splt[] = DBSystem.splitTableRow(node.mObjects[i]
								.toString());
						for (String s : splt) {
							hset.add(s);
						}
					}
				}
				node = node.mNextNode;
			}
		} else if (l && !h) {
			while (node != null) {
				for (int i = 0; i < node.mNumKeys; i++) {
					if (compare(node.mKeys[i], high) < 0
							&& compare(node.mKeys[i], low) >= 0) {
						String splt[] = DBSystem.splitTableRow(node.mObjects[i]
								.toString());
						for (String s : splt) {
							hset.add(s);
						}
					}
				}
				node = node.mNextNode;
			}
		} else {
			while (node != null) {
				for (int i = 0; i < node.mNumKeys; i++) {
					if (compare(node.mKeys[i], high) < 0
							&& compare(node.mKeys[i], low) > 0) {
						String splt[] = DBSystem.splitTableRow(node.mObjects[i]
								.toString());
						for (String s : splt) {
							hset.add(s);
						}
					}
				}
				node = node.mNextNode;
			}
		}
		return hset;
	}

	public Object search2(Node node, String key) {
		while (node != null) {
			int i = 0;
			while (i < node.mNumKeys && compare(key, node.mKeys[i]) > 0) {
				i++;
			}
			if (i < node.mNumKeys && compare(key, node.mKeys[i]) == 0) {
				return node.mObjects[i];
			}
			if (node.mIsLeafNode) {
				return null;
			} else {
				node = node.mChildNodes[i];
			}
		}
		return null;
	}

	public Object search2(String key) {
		return search2(mRootNode, key);
	}

	// Inorder walk over the tree.
	public String toString() {
		String string = "";
		Node node = mRootNode;
		while (!node.mIsLeafNode) {
			node = node.mChildNodes[0];
		}
		while (node != null) {
			for (int i = 0; i < node.mNumKeys; i++) {
				string += node.mObjects[i] + ", ";
			}
			node = node.mNextNode;
		}
		return string;
	}

	// Inorder walk over parts of the tree.
	public String toString(String fromKey, String toKey) {
		String string = "";
		Node node = getLeafNodeForKey(fromKey);
		while (node != null) {
			int j = 0;
			for (j = 0; compare(node.mKeys[j], fromKey) < 0; j++)
				;
			for (; j < node.mNumKeys; j++) {

				if (compare(node.mKeys[j], toKey) > 0) {
					return string;
				} else
					string += node.mObjects[j] + ", ";
			}
			node = node.mNextNode;
		}
		return string;
	}

	public String toStringBoundaryLessThan(String fromKey, String toKey) {
		String string = "";
		Node node = getLeafNodeForKey(fromKey);
		while (node != null) {
			int j = 0;
			for (j = 0; compare(node.mKeys[j], fromKey) < 0; j++)
				;
			for (; j < node.mNumKeys; j++) {

				if (compare(node.mKeys[j], toKey) >= 0) {
					return string;
				} else
					string += node.mObjects[j] + ", ";
			}
			node = node.mNextNode;
		}
		return string;
	}

	Node getLeafNodeForKey(String key) {
		Node node = mRootNode;
		int flag = 0;
		while (node != null) {
			int i = 0;
			while (i < node.mNumKeys && compare(key, node.mKeys[i]) > 0) {
				i++;
			}
			if (i < node.mNumKeys) {
				if (compare(key, node.mKeys[i]) == 0) {
					if (node.mChildNodes[i + 1] == null)
						return node;
					node = node.mChildNodes[i + 1];

					while (node != null && !node.mIsLeafNode) {
						node = node.mChildNodes[0];
					}

					return node;
				} else if (flag == 0 && !node.mIsLeafNode) {
					flag = 1;
					node = node.mChildNodes[0];
					continue;
				}

			}
			if (node.mIsLeafNode) {
				return null;
			} else {
				node = node.mChildNodes[i];
			}
		}
		return null;
	}

	public static void intialiseBPTree() {
		for (Entry<String, tableInfo> entry : DBSystem.pageTree.entrySet()) {
			try {
				File table = new File(DBSystem.pathForData + "/"
						+ entry.getKey() + ".csv");
				// System.out.println(DBSystem.pathForData + "/" +
				// entry.getKey()
				// + ".csv");
				FileReader fr = new FileReader(table);
				BufferedReader br = new BufferedReader(fr);
				String line = "";
				LinkedHashMap<String, BPlusTree> columnStructure = new LinkedHashMap<String, BPlusTree>();

				for (Entry<String, String> colentry : DBSystem.pageTree
						.get(entry.getKey()).coltypepair.entrySet()) {
					BPlusTree bpTree = new BPlusTree();
					bpTree.columnType = colentry.getValue().trim();
					columnStructure.put(colentry.getKey(), bpTree);
				}
				int lineNumber = 0;
				while ((line = br.readLine()) != null) {
					// System.out.println("----" + line);
					String str[] = DBSystem.splitTableRow(line);
					int i = 0;
					for (Entry<String, String> colentry : DBSystem.pageTree
							.get(entry.getKey()).coltypepair.entrySet()) {

						// System.out.println("!!str[" + i + "]=" + str[i]);

						columnStructure.get(colentry.getKey())
								.addWithDuplicationHandled(str[i],
										lineNumber + "");
						// System.out.println("---out---");
						i++;
					}
					lineNumber++;
				}
				bPlusTreeStructure.put(entry.getKey(), columnStructure);

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static Long getDistinctCount() {
		return null;
	}

	public static void main(String[] args) {
		BPlusTree bPlusTree = new BPlusTree();
		/*
		 * int primeNumbers[] = new int[] { 2, 3, 5, 7, 11, 13, 19, 23, 37, 41,
		 * 43, 47, 53, 59, 67, 71, 61, 73, 79, 89, 97, 101, 103, 109, 29, 31,
		 * 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 17, 83,
		 * 107 };
		 * 
		 * /*for (int i = 0; i < primeNumbers.length; i++) {
		 * bPlusTree.add(primeNumbers[i], String.valueOf(primeNumbers[i])); }
		 */
		bPlusTree.columnType = "integer";

		String str[] = new String[] { "0", "1", "2", "3", "3", "5", "6", "7",
				"8", "9", "9.2" };
		for (int i = 0; i < str.length; i++) {
			bPlusTree.addWithDuplicationHandled(str[i], i);
		}

		bPlusTree.addWithDuplicationHandled("3", "12");
		bPlusTree.addWithDuplicationHandled("3", "13");
		bPlusTree.addWithDuplicationHandled("3", "14");
		bPlusTree.addWithDuplicationHandled("7", "70");
		bPlusTree.addWithDuplicationHandled("9", "90");
		bPlusTree.addWithDuplicationHandled("9.2", "92");
		bPlusTree.addWithDuplicationHandled("0", "00");

		bPlusTree.addWithDuplicationHandled("3", "15");
		bPlusTree.addWithDuplicationHandled("3", "16");
		bPlusTree.addWithDuplicationHandled("3", "17");
		bPlusTree.addWithDuplicationHandled("3", "18");

		// System.out.println(bPlusTree.search("2"));
		System.out.println("---" + bPlusTree.toString());
		System.out.println(bPlusTree.rangeQuery("1", "18", false, true));

		/*
		 * bPlusTree.columnType="integer"; String str[]=new
		 * String[]{"0","1","2","3","4","5","6","7","8","9"}; for (int i = 0; i
		 * < str.length; i++) { bPlusTree.add(i+"", str[i]); }
		 * 
		 * System.out.println(bPlusTree.search("1"));
		 * System.out.println(bPlusTree.toString());
		 * System.out.println(bPlusTree.toString("-1","1789"));
		 */
		/*
		 * for (int i = 0; i < primeNumbers.length; i++) { String value =
		 * String.valueOf(primeNumbers[i]); Object searchResult = (Object)
		 * bPlusTree.search(primeNumbers[i]); if (!value.equals(searchResult)) {
		 * System.out.println("Oops: Key " + primeNumbers[i] +
		 * " retrieved object " + searchResult); } }
		 */

		// .addWithDuplicationHandled(11, "a");
		// bPlusTree.addWithDuplicationHandled(11, "b");

		// System.out.println(bPlusTree.search("11"));
		// Object a=bPlusTree.search(11);
		// Node x=(Node)a;
		// System.out.println(bPlusTree.search(17));
		// System.out.println(bPlusTree.toString(17, 179));
		/*
		 * bPlusTree.add(17, "a"); bPlusTree.add(17, "b"); bPlusTree.add(17,
		 * "c"); bPlusTree.add(17, "d"); bPlusTree.add(17, "e");
		 * bPlusTree.add(17, "f"); bPlusTree.add(17, "g"); bPlusTree.add(17,
		 * "h"); bPlusTree.add(17, "i"); bPlusTree.add(17, "i");
		 * bPlusTree.add(17, "i"); bPlusTree.add(17, "i"); bPlusTree.add(17,
		 * "i"); bPlusTree.add(17, "i"); bPlusTree.add(17, "i");
		 * bPlusTree.add(17, "i"); bPlusTree.add(17, "i"); bPlusTree.add(17,
		 * "i"); bPlusTree.add(17, "i"); bPlusTree.add(17, "i");
		 * bPlusTree.add(17, "i"); bPlusTree.add(17, "i"); bPlusTree.add(17,
		 * "i"); bPlusTree.add(17, "i"); bPlusTree.add(17, "i");
		 * bPlusTree.add(17, "i"); bPlusTree.add(17, "i"); bPlusTree.add(17,
		 * "i"); bPlusTree.add(17, "i"); bPlusTree.add(17, "i");
		 * bPlusTree.add(17, "i");
		 */

		// System.out.println(bPlusTree.toString(2, 179));

	}
}