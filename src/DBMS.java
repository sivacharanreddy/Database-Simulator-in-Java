import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Map.Entry;

/**
 * CS 267 - Project - Implements create index, drop index, list table, and
 * exploit the index in select statements.
 */
public class DBMS {
	private static final String COMMAND_FILE_LOC = ".\\src\\Commands.txt";
	private static final String OUTPUT_FILE_LOC = ".\\src\\Output.txt";

	private static final String TABLE_FOLDER_NAME = ".\\src\\tables";
	private static final String TABLE_FILE_EXT = ".tab";
	private static final String INDEX_FILE_EXT = ".idx";

	public static boolean ASC = true;
	public static boolean DESC = false;

	private DbmsPrinter out;
	private ArrayList<Table> tables;

	public DBMS() {
		tables = new ArrayList<Table>();
	}

	/**
	 * Main method to run the DBMS engine.
	 * 
	 * @param args
	 *            arg[0] is input file, arg[1] is output file.
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		DBMS db = new DBMS();
		db.out = new DbmsPrinter();
		Scanner in = null;
		try {
			// set input file
			if (args.length > 0) {
				in = new Scanner(new File(args[0]));
			} else {
				in = new Scanner(new File(COMMAND_FILE_LOC));
			}

			// set output files
			if (args.length > 1) {
				db.out.addPrinter(args[1]);
			} else {
				db.out.addPrinter(OUTPUT_FILE_LOC);
			}

			// Load data to memory
			db.loadTables();

			// Go through each line in the Command.txt file
			while (in.hasNextLine()) {
				String sql = in.nextLine();
				StringTokenizer tokenizer = new StringTokenizer(sql);

				// Evaluate the SQL statement
				if (tokenizer.hasMoreTokens()) {
					String command = tokenizer.nextToken();
					if (command.equalsIgnoreCase("CREATE")) {
						if (tokenizer.hasMoreTokens()) {
							command = tokenizer.nextToken();
							if (command.equalsIgnoreCase("TABLE")) {
								db.createTable(sql, tokenizer);
							} else if (command.equalsIgnoreCase("UNIQUE")) {
								command = tokenizer.nextToken();
								if (command.equalsIgnoreCase("INDEX")) {
									// TODO your PART 1 code goes here
									db.createIndex(sql, tokenizer, true);
								} else {
									throw new DbmsError(
											"Invalid CREATE UNIQUE " + command
													+ " statement. '" + sql
													+ "'.");
								}
							} else if (command.equalsIgnoreCase("INDEX")) {
								// TODO your PART 1 code goes here
								db.createIndex(sql, tokenizer, false);
							} else {
								throw new DbmsError("Invalid CREATE " + command
										+ " statement. '" + sql + "'.");
							}
						} else {
							throw new DbmsError("Invalid CREATE statement. '"
									+ sql + "'.");
						}
					} else if (command.equalsIgnoreCase("INSERT")) {
						db.insertInto(sql, tokenizer);
					} else if (command.equalsIgnoreCase("DROP")) {
						if (tokenizer.hasMoreTokens()) {
							command = tokenizer.nextToken();
							if (command.equalsIgnoreCase("TABLE")) {
								db.dropTable(sql, tokenizer);
							} else if (command.equalsIgnoreCase("INDEX")) {
								db.dropIndex(sql, tokenizer);
							} else {
								throw new DbmsError("Invalid DROP " + command
										+ " statement. '" + sql + "'.");
							}
						} else {
							throw new DbmsError("Invalid DROP statement. '"
									+ sql + "'.");
						}
					} else if (command.equalsIgnoreCase("RUNSTATS")) {

						String tableName = tokenizer.nextToken();

						db.runStats(tableName, sql, tokenizer);
						db.printRunstats(tableName);

					} else if (command.equalsIgnoreCase("SELECT")) {
						// TODO your PART 2 code goes here
					} else if (command.equalsIgnoreCase("--")) {
						// Ignore this command as a comment
					} else if (command.equalsIgnoreCase("COMMIT")) {
						try {
							// Check for ";"
							if (!tokenizer.nextElement().equals(";")) {
								throw new NoSuchElementException();
							}

							// Check if there are more tokens
							if (tokenizer.hasMoreTokens()) {
								throw new NoSuchElementException();
							}

							// Save tables to files
							for (Table table : db.tables) {
								db.storeTableFile(table);
							}
						} catch (NoSuchElementException ex) {
							throw new DbmsError("Invalid COMMIT statement. '"
									+ sql + "'.");
						}
					} else {
						throw new DbmsError("Invalid statement. '" + sql + "'.");
					}
				}
			}

			// Save tables to files
			for (Table table : db.tables) {
				db.storeTableFile(table);
			}
		} catch (DbmsError ex) {
			db.out.println("DBMS ERROR:  " + ex.getMessage());
			ex.printStackTrace();
		} catch (Exception ex) {
			db.out.println("JAVA ERROR:  " + ex.getMessage());
			ex.printStackTrace();
		} finally {
			// clean up
			try {
				in.close();
			} catch (Exception ex) {
			}

			try {
				db.out.cleanup();
			} catch (Exception ex) {
			}
		}
	}

	/**
	 * Loads tables to memory
	 * 
	 * @throws Exception
	 */
	private void loadTables() throws Exception {
		// Get all the available tables in the "tables" directory
		File tableDir = new File(TABLE_FOLDER_NAME);
		if (tableDir.exists() && tableDir.isDirectory()) {
			for (File tableFile : tableDir.listFiles()) {
				// For each file check if the file extension is ".tab"
				String tableName = tableFile.getName();
				int periodLoc = tableName.lastIndexOf(".");
				String tableFileExt = tableName.substring(tableName
						.lastIndexOf(".") + 1);
				if (tableFileExt.equalsIgnoreCase("tab")) {
					// If it is a ".tab" file, create a table structure
					Table table = new Table(tableName.substring(0, periodLoc));
					Scanner in = new Scanner(tableFile);

					try {
						// Read the file to get Column definitions
						int numCols = Integer.parseInt(in.nextLine());

						for (int i = 0; i < numCols; i++) {
							StringTokenizer tokenizer = new StringTokenizer(
									in.nextLine());
							String name = tokenizer.nextToken();
							String type = tokenizer.nextToken();
							boolean nullable = Boolean.parseBoolean(tokenizer
									.nextToken());
							switch (type.charAt(0)) {
							case 'C':
								table.addColumn(new Column(i + 1, name,
										Column.ColType.CHAR, Integer
												.parseInt(type.substring(1)),
										nullable));
								break;
							case 'I':
								table.addColumn(new Column(i + 1, name,
										Column.ColType.INT, 4, nullable));
								break;
							default:
								break;
							}
						}

						// Read the file for index definitions
						int numIdx = Integer.parseInt(in.nextLine());
						for (int i = 0; i < numIdx; i++) {
							StringTokenizer tokenizer = new StringTokenizer(
									in.nextLine());
							Index index = new Index(tokenizer.nextToken());
							index.setIsUnique(Boolean.parseBoolean(tokenizer
									.nextToken()));

							int idxColPos = 1;
							while (tokenizer.hasMoreTokens()) {
								String colDef = tokenizer.nextToken();
								Index.IndexKeyDef def = index.new IndexKeyDef();
								def.idxColPos = idxColPos;
								def.colId = Integer.parseInt(colDef.substring(
										0, colDef.length() - 1));
								switch (colDef.charAt(colDef.length() - 1)) {
								case 'A':
									def.descOrder = false;
									break;
								case 'D':
									def.descOrder = true;
									break;
								default:
									break;
								}

								index.addIdxKey(def);
							}

							table.addIndex(index);
							loadIndex(table, index);
						}

						// Read the data from the file
						int numRows = Integer.parseInt(in.nextLine());
						for (int i = 0; i < numRows; i++) {
							table.addData(in.nextLine());
						}
					} catch (DbmsError ex) {
						throw ex;
					} catch (Exception ex) {
						throw new DbmsError("Invalid table file format.");
					} finally {
						in.close();
					}
					tables.add(table);
				}
			}
		} else {
			throw new FileNotFoundException(
					"The system cannot find the tables directory specified.");
		}
	}

	/**
	 * Loads specified table to memory
	 * 
	 * @throws DbmsError
	 */
	private void loadIndex(Table table, Index index) throws DbmsError {
		try {
			Scanner in = new Scanner(new File(TABLE_FOLDER_NAME,
					table.getTableName() + index.getIdxName() + INDEX_FILE_EXT));
			String def = in.nextLine();
			String rows = in.nextLine();

			while (in.hasNext()) {
				String line = in.nextLine();
				Index.IndexKeyVal val = index.new IndexKeyVal();
				val.rid = Integer.parseInt(new StringTokenizer(line)
						.nextToken());
				val.value = line.substring(line.indexOf("'") + 1,
						line.lastIndexOf("'"));
				index.addKey(val);
			}
			in.close();
		} catch (Exception ex) {
			throw new DbmsError("Invalid index file format.");
		}
	}

	/**
	 * CREATE TABLE
	 * <table name>
	 * ( <col name> < CHAR ( length ) | INT > <NOT NULL> ) ;
	 * 
	 * @param sql
	 * @param tokenizer
	 * @throws Exception
	 */
	private void createTable(String sql, StringTokenizer tokenizer)
			throws Exception {
		try {
			// Check the table name
			String tok = tokenizer.nextToken().toUpperCase();
			if (Character.isAlphabetic(tok.charAt(0))) {
				// Check if the table already exists
				for (Table tab : tables) {
					if (tab.getTableName().equals(tok)) {
						throw new DbmsError("Table " + tok
								+ "already exists. '" + sql + "'.");
					}
				}

				// Create a table instance to store data in memory
				Table table = new Table(tok.toUpperCase());

				// Check for '('
				tok = tokenizer.nextToken();
				if (tok.equals("(")) {
					// Look through the column definitions and add them to the
					// table in memory
					boolean done = false;
					int colId = 1;
					while (!done) {
						tok = tokenizer.nextToken();
						if (Character.isAlphabetic(tok.charAt(0))) {
							String colName = tok;
							Column.ColType colType = Column.ColType.INT;
							int colLength = 4;
							boolean nullable = true;

							tok = tokenizer.nextToken();
							if (tok.equalsIgnoreCase("INT")) {
								// use the default Column.ColType and colLength

								// Look for NOT NULL or ',' or ')'
								tok = tokenizer.nextToken();
								if (tok.equalsIgnoreCase("NOT")) {
									// look for NULL after NOT
									tok = tokenizer.nextToken();
									if (tok.equalsIgnoreCase("NULL")) {
										nullable = false;
									} else {
										throw new NoSuchElementException();
									}

									tok = tokenizer.nextToken();
									if (tok.equals(",")) {
										// Continue to the next column
									} else if (tok.equalsIgnoreCase(")")) {
										done = true;
									} else {
										throw new NoSuchElementException();
									}
								} else if (tok.equalsIgnoreCase(",")) {
									// Continue to the next column
								} else if (tok.equalsIgnoreCase(")")) {
									done = true;
								} else {
									throw new NoSuchElementException();
								}
							} else if (tok.equalsIgnoreCase("CHAR")) {
								colType = Column.ColType.CHAR;

								// Look for column length
								tok = tokenizer.nextToken();
								if (tok.equals("(")) {
									tok = tokenizer.nextToken();
									try {
										colLength = Integer.parseInt(tok);
									} catch (NumberFormatException ex) {
										throw new DbmsError(
												"Invalid table column length for "
														+ colName + ". '" + sql
														+ "'.");
									}

									// Check for the closing ')'
									tok = tokenizer.nextToken();
									if (!tok.equals(")")) {
										throw new DbmsError(
												"Invalid table column definition for "
														+ colName + ". '" + sql
														+ "'.");
									}

									// Look for NOT NULL or ',' or ')'
									tok = tokenizer.nextToken();
									if (tok.equalsIgnoreCase("NOT")) {
										// Look for NULL after NOT
										tok = tokenizer.nextToken();
										if (tok.equalsIgnoreCase("NULL")) {
											nullable = false;

											tok = tokenizer.nextToken();
											if (tok.equals(",")) {
												// Continue to the next column
											} else if (tok
													.equalsIgnoreCase(")")) {
												done = true;
											} else {
												throw new NoSuchElementException();
											}
										} else {
											throw new NoSuchElementException();
										}
									} else if (tok.equalsIgnoreCase(",")) {
										// Continue to the next column
									} else if (tok.equalsIgnoreCase(")")) {
										done = true;
									} else {
										throw new NoSuchElementException();
									}
								} else {
									throw new DbmsError(
											"Invalid table column definition for "
													+ colName + ". '" + sql
													+ "'.");
								}
							} else {
								throw new NoSuchElementException();
							}

							// Everything is ok. Add the column to the table
							table.addColumn(new Column(colId, colName, colType,
									colLength, nullable));
							colId++;
						} else {
							// if(colId == 1) {
							throw new DbmsError(
									"Invalid table column identifier " + tok
											+ ". '" + sql + "'.");
							// }
						}
					}

					// Check for the semicolon
					tok = tokenizer.nextToken();
					if (!tok.equals(";")) {
						throw new NoSuchElementException();
					}

					// Check if there are more tokens
					if (tokenizer.hasMoreTokens()) {
						throw new NoSuchElementException();
					}

					if (table.getNumColumns() == 0) {
						throw new DbmsError(
								"No column descriptions specified. '" + sql
										+ "'.");
					}

					// The table is stored into memory when this program exists.
					tables.add(table);

					out.println("Table " + table.getTableName()
							+ " was created.");
				} else {
					throw new NoSuchElementException();
				}
			} else {
				throw new DbmsError("Invalid table identifier " + tok + ". '"
						+ sql + "'.");
			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid CREATE TABLE statement. '" + sql
					+ "'.");
		}
	}

	/**
	 * INSERT INTO
	 * <table name>
	 * VALUES ( val1 , val2, .... ) ;
	 * 
	 * @param sql
	 * @param tokenizer
	 * @throws Exception
	 */
	private void insertInto(String sql, StringTokenizer tokenizer)
			throws Exception {
		try {
			String tok = tokenizer.nextToken();
			if (tok.equalsIgnoreCase("INTO")) {
				tok = tokenizer.nextToken().trim().toUpperCase();
				Table table = null;
				for (Table tab : tables) {
					if (tab.getTableName().equals(tok)) {
						table = tab;
						break;
					}
				}

				if (table == null) {
					throw new DbmsError("Table " + tok + " does not exist.");
				}

				tok = tokenizer.nextToken();
				if (tok.equalsIgnoreCase("VALUES")) {
					tok = tokenizer.nextToken();
					if (tok.equalsIgnoreCase("(")) {
						tok = tokenizer.nextToken();
						String values = String.format("%3s", table.getData()
								.size() + 1)
								+ " ";
						int colId = 0;
						boolean done = false;
						while (!done) {
							if (tok.equals(")")) {
								done = true;
								break;
							} else if (tok.equals(",")) {
								// Continue to the next value
							} else {
								if (colId == table.getNumColumns()) {
									throw new DbmsError(
											"Invalid number of values were given.");
								}

								Column col = table.getColumns().get(colId);

								if (tok.equals("-") && !col.isColNullable()) {
									throw new DbmsError(
											"A NOT NULL column cannot have null. '"
													+ sql + "'.");
								}

								if (col.getColType() == Column.ColType.INT) {
									try {
										int temp = Integer.parseInt(tok);
									} catch (Exception ex) {
										throw new DbmsError(
												"An INT column cannot hold a CHAR. '"
														+ sql + "'.");
									}

									tok = String.format("%10s", tok.trim());
								} else if (col.getColType() == Column.ColType.CHAR) {
									int length = tok.length();
									if (length > col.getColLength()) {
										throw new DbmsError(
												"A CHAR column cannot exceede its length. '"
														+ sql + "'.");
									}

									tok = String.format(
											"%-" + col.getColLength() + "s",
											tok.trim());
								}

								values += tok + " ";
								colId++;
							}
							tok = tokenizer.nextToken().trim();
						}

						if (colId != table.getNumColumns()) {
							throw new DbmsError(
									"Invalid number of values were given.");
						}

						// Check for the semicolon
						tok = tokenizer.nextToken();
						if (!tok.equals(";")) {
							throw new NoSuchElementException();
						}

						// Check if there are more tokens
						if (tokenizer.hasMoreTokens()) {
							throw new NoSuchElementException();
						}

						// insert the value to table
						table.addData(values);
						out.println("One line was saved to the table. "
								+ table.getTableName() + ": " + values);
					} else {
						throw new NoSuchElementException();
					}
				} else {
					throw new NoSuchElementException();
				}
			} else {
				throw new NoSuchElementException();
			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid INSERT INTO statement. '" + sql + "'.");
		}
	}

	/**
	 * DROP TABLE
	 * <table name>
	 * ;
	 * 
	 * @param sql
	 * @param tokenizer
	 * @throws Exception
	 */
	private void dropTable(String sql, StringTokenizer tokenizer)
			throws Exception {
		try {
			// Get table name
			String tableName = tokenizer.nextToken();

			// Check for the semicolon
			String tok = tokenizer.nextToken();
			if (!tok.equals(";")) {
				throw new NoSuchElementException();
			}

			// Check if there are more tokens
			if (tokenizer.hasMoreTokens()) {
				throw new NoSuchElementException();
			}

			// Delete the table if everything is ok
			boolean dropped = false;
			for (Table table : tables) {
				if (table.getTableName().equalsIgnoreCase(tableName)) {
					table.delete = true;
					dropped = true;
					break;
				}
			}

			/*
			 * if (dropped) { out.println("Table " + tableName +
			 * " does not exist."); } else { out.println("Table " + tableName +
			 * " was dropped."); }
			 */
			if (dropped) {
				out.println("Table " + tableName + " was dropped.");

			} else {
				out.println("Table " + tableName + " does not exist.");
			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid DROP TABLE statement. '" + sql + "'.");
		}

	}

	private void printRunstats(String tableName) {
		for (Table table : tables) {
			if (table.getTableName().equals(tableName)) {
				out.println("TABLE CARDINALITY: " + table.getTableCard());
				for (Column column : table.getColumns()) {
					out.println(column.getColName());
					out.println("\tCOLUMN CARDINALITY: " + column.getColCard());
					out.println("\tCOLUMN HIGH KEY: " + column.getHiKey());
					out.println("\tCOLUMN LOW KEY: " + column.getLoKey());
				}
				break;
			}
		}
	}

	private void storeTableFile(Table table) throws FileNotFoundException {
		File tableFile = new File(TABLE_FOLDER_NAME, table.getTableName()
				+ TABLE_FILE_EXT);

		// Delete the file if it was marked for deletion
		if (table.delete) {
			try {
				tableFile.delete();
			} catch (Exception ex) {
				out.println("Unable to delete table file for "
						+ table.getTableName() + ".");
			}
		} else {
			// Create the table file writer
			PrintWriter out = new PrintWriter(tableFile);

			// Write the column descriptors
			out.println(table.getNumColumns());
			for (Column col : table.getColumns()) {
				if (col.getColType() == Column.ColType.INT) {
					out.println(col.getColName() + " I " + col.isColNullable());
				} else if (col.getColType() == Column.ColType.CHAR) {
					out.println(col.getColName() + " C" + col.getColLength()
							+ " " + col.isColNullable());
				}
			}

			// Write the index info
			out.println(table.getNumIndexes());

			for (Index index : table.getIndexes()) {

				if (!index.delete) {
					String idxInfo = index.getIdxName() + " "
							+ index.getIsUnique() + " ";

					for (Index.IndexKeyDef def : index.getIdxKey()) {
						idxInfo += def.colId;
						if (def.descOrder) {
							idxInfo += "D ";
						} else {
							idxInfo += "A ";
						}
					}
					out.println(idxInfo);
				}
			}

			// Write the rows of data
			out.println(table.getData().size());
			for (String data : table.getData()) {
				out.println(data);
			}

			out.flush();
			out.close();
		}

		// Save indexes to file
		for (Index index : table.getIndexes()) {

			File indexFile = new File(TABLE_FOLDER_NAME, table.getTableName()
					+ index.getIdxName() + INDEX_FILE_EXT);

			// Delete the file if it was marked for deletion
			if (index.delete) {
				try {
					indexFile.delete();
				} catch (Exception ex) {
					out.println("Unable to delete index file for "
							+ indexFile.getName() + ".");
				}
			} else {
				PrintWriter out = new PrintWriter(indexFile);
				String idxInfo = index.getIdxName() + " " + index.getIsUnique()
						+ " ";

				// Write index definition
				for (Index.IndexKeyDef def : index.getIdxKey()) {
					idxInfo += def.colId;
					if (def.descOrder) {
						idxInfo += "D ";
					} else {
						idxInfo += "A ";
					}
				}
				out.println(idxInfo);

				// Write index keys
				out.println(index.getKeys().size());
				for (Index.IndexKeyVal key : index.getKeys()) {
					String rid = String.format("%3s", key.rid);
					out.println(rid + " '" + key.value + "'");
					// out.println(key.rid + " '" + key.value + "'");
				}

				out.flush();
				out.close();

			}
		}

	}

	private void createIndex(String sql, StringTokenizer tokenizer,
			boolean isUnique) throws Exception {

		// get idxname
		String idxName = tokenizer.nextToken().toUpperCase();
		// System.out.println("\n in create idc = " + idxName);

		// create idx object
		Index index = new Index(idxName);

		// verify if index exists

		for (Table tab : tables) {
			if (isIndexPresent(tab, index)) {
				System.out.println("\nError: Indexname " + index.getIdxName()
						+ " already exists!!!");
				System.exit(0);
			}
		}

		// escape ON keyword
		String on = tokenizer.nextToken().toUpperCase();

		// get tablename
		String tableName = tokenizer.nextToken().toUpperCase();

		// verify if table exists
		boolean isTableExist = false;
		for (Table tab : tables) {
			if (tab.getTableName().equalsIgnoreCase(tableName)) {
				isTableExist = true;
				break;
			}
		}

		// exit if table does not exist
		if (!isTableExist) {
			System.out.println("\nError: Table " + tableName
					+ " does not exist!!!");
			System.exit(0);
		}

		// System.out.println("\n in create idc = " + tableName);

		// get table object from loaded tables array
		Table referenceTable = null;
		for (Table tab : tables) {
			if (tab.getTableName().equals(tableName)) {
				referenceTable = tab;
			}
		}

		// escape '(' keyword
		String temp = tokenizer.nextToken().toUpperCase();

		// parse columns and make IdxDef objects for Index object
		int colsPos = 1;
		boolean escapeToken = false;
		String columnName = "";

		while (tokenizer.hasMoreTokens()) {

			// Read token
			if (!escapeToken)
				columnName = tokenizer.nextToken();

			// create new IndexDef object
			Index.IndexKeyDef def = index.new IndexKeyDef();

			// System.out.println("\n in create idx col = " + columnName);

			// If ')'token, then exit the parsing loop
			if (columnName.equals(")"))
				break;
			else if (columnName.equals(",")) { // parsethe next token by setting
												// escapeToken false
				escapeToken = false;
				continue;
			}

			// Set IndexColPos and then increment colsPos by 1 for dealing with
			// next IndexColPos
			def.idxColPos = colsPos++;

			boolean isIdxColPresentOnTable = false;

			// get colId for current columnname
			for (Column col : referenceTable.getColumns()) {
				// System.out.println("\ncol = " + columnName);
				if (col.getColName().equalsIgnoreCase(columnName)) {

					isIdxColPresentOnTable = true;
					// Set ColId
					def.colId = col.getColId();
					// System.out.println("\ncol = " + columnName + " and id = "
					// + def.colId);
					break;
				}
			}

			// Exit if column mentioned in index does not exist
			if (!isIdxColPresentOnTable) {
				System.out.println("\nError: Column " + columnName
						+ " does not exist on Table "
						+ referenceTable.getTableName());
				System.exit(0);
			}

			// set ORDER i.e. ASC / DESC
			if (tokenizer.hasMoreTokens()) {
				String order = tokenizer.nextToken();
				if (order.equalsIgnoreCase("DESC")) {
					def.descOrder = true;
					// System.out.println("DESC it is for " + columnName);
					escapeToken = false;
				} else {
					def.descOrder = false;
					columnName = order;
					// System.out.println("in ASC and nxt col = " + columnName);
					escapeToken = true;
				}
			}

			// Add IndexDef object to Index object
			index.addIdxKey(def);
		}

		// set uniqe value CHANGE
		index.setIsUnique(isUnique);

		// Add Index object to Table Object
		referenceTable.addIndex(index);

		// HashMap for RID -> Value
		Map<Integer, String> unsortMap = new HashMap<Integer, String>();

		// Load data of the Table to ArrayList
		ArrayList<String> data = referenceTable.getData();

		// Create Index Contents as RID->Value by iteerating ob every row of
		// table data
		for (String s : data) {
			// System.out.println("\nline = " + s);

			StringTokenizer tokenizer1 = new StringTokenizer(s);

			// get RID
			String ridStr = tokenizer1.nextToken();
			int rid = Integer.parseInt(ridStr);

			// Store the field values in the row in val_ref array
			String val_ref[] = new String[100];
			int k = 1;
			while (tokenizer1.hasMoreTokens()) {
				val_ref[k++] = tokenizer1.nextToken();
			}

			// Based on indexDef array and make the value for RID->value
			// get value
			String value = "";
			String temp_val = "";

			for (Index.IndexKeyDef def : index.getIdxKey()) {

				// get Column Id for IndexDef
				int colId = def.colId;

				/*
				 * System.out.println("\ncol no = " + colId);
				 * System.out.println("\nrelevant val b4 adjust= " +
				 * val_ref[colId]);
				 */
				for (Column col : referenceTable.getColumns()) {
					// System.out.println("\ncol id = " + colId);
					if (col.getColId() == colId) {
						temp_val = val_ref[colId];
						/*
						 * System.out .println("got column and valye = " +
						 * temp_val); System.out.println("col type = " +
						 * col.getColType());
						 */

						int len = col.getColLength() - temp_val.length();

						// handle NULL values
						if (temp_val.equals("-")) {

							if (def.descOrder) {
								temp_val = "£";
							}

							if (col.getColType().toString().equals("CHAR")) {
								for (int m = 0; m < len; m++) {
									temp_val += " ";
								}
							} else {
								if (temp_val.equals("£")) {
									temp_val = "~";
								}
								else
								{
									len = 10 - temp_val.length();
									for (int m = 0; m < len; m++) {
										temp_val = " " + temp_val;
									}
								}
							}

						} else {

							// If column tyoe is String, then Adjust by adding
							// extra spaces
							if (col.getColType().toString().equals("CHAR")) {
								/*
								 * System.out.println("len = " +
								 * (col.getColLength() - temp_val.length()));
								 */

								// System.out.println("\n len = " + len);
								// If DESC then invert using Z/z reference
								if (def.descOrder) {

									char[] arr = temp_val.toCharArray();
									temp_val = "";
									for (char c : arr) {
										char new_c;
										if (Character.isUpperCase(c)) {
											new_c = (char) (90 - ((int) c % 65));
											temp_val += new_c;
										} else {
											new_c = (char) (122 - ((int) c % 97));
											temp_val += new_c;
										}
									}

								}
								// System.out.println("\n len = " + len);
								for (int m = 0; m < len; m++) {
									temp_val += " ";
								}

								/*
								 * System.out.println("after adjust spaces = " +
								 * temp_val);
								 */
							} else if (col.getColType().toString()
									.equals("INT")) {
								// System.out.println("len = " + (10 -
								// temp_val.length()));
								len = 10 - temp_val.length();
								if (def.descOrder) {
									// System.out.println("in DESC for INT");
									long num = Long.parseLong(temp_val);
									num = Long.parseLong("9999999999") - num;
									// System.out.println("DESC value of " +
									// temp_val + " is " + num);
									temp_val = Long.toString(num);
								} else {
									for (int m = 0; m < len; m++) {
										temp_val = "0" + temp_val;
									}
								}
								/*
								 * System.out.println("after adjust spaces = " +
								 * temp_val);
								 */
							}
						}
						break;
					}
				}

				// COncat values for all IndexDef objects to make final Value
				value += temp_val;
			}

			// reomove extra last space added by for loop of whitespace
			value = value.replaceFirst(" $", "");

			// System.out.println(rid + " => " + value);

			// add RID->Value
			unsortMap.put(rid, value);
		}

		Map<Integer, String> sortedMapAsc = new HashMap<Integer, String>();

		// SORT the map i.e. Index Contents
		sortedMapAsc = sortByComparator(unsortMap, ASC);

		// Check for UNIQE INDEX
		if (isUnique) {
			List<String> checkUnique = new ArrayList<String>(
					sortedMapAsc.values());
			boolean isUniqueAllowed = true;

			for (int h = 0; h < checkUnique.size() - 1; h++) {
				if (checkUnique.get(h).equals(checkUnique.get(h + 1))) {
					isUniqueAllowed = false;
					break;
				}
			}

			if (!isUniqueAllowed) {
				System.out.println("\nERROR: Unique Index not allowed");
				System.exit(0);
			}
		}

		for (Map.Entry<Integer, String> entry : sortedMapAsc.entrySet()) {
			int rid = entry.getKey();
			String value = entry.getValue();
			Index.IndexKeyVal key_val = index.new IndexKeyVal();
			key_val.rid = rid;
			key_val.value = value;

			/*if (value.equals("")~
					value = value.replaceAll("(.*)~(.*)", "         £); 
			*/
			if (value.matches("(.*)~(.*)")) {
					value = value.replaceAll("(.*)~(.*)", "$1         £$2");
							key_val.value = value;
			}				
			/*if (value.matches("£(\\w+)(.*)")) {
				value = value.replaceAll("£(\\w+)(.*)", "         £$1$2");
				key_val.value = value;
			}*/
				
			index.addKey(key_val);
		}

		System.out.println("\nIndex " + index.getIdxName()
				+ " is created successfuly On Table "
				+ referenceTable.getTableName());
	}

	private static Map<Integer, String> sortByComparator(
			Map<Integer, String> unsortMap, final boolean order) {

		List<Entry<Integer, String>> list = new LinkedList<Entry<Integer, String>>(
				unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<Integer, String>>() {
			public int compare(Entry<Integer, String> o1,
					Entry<Integer, String> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		// Maintaining insertion order with the help of LinkedList
		Map<Integer, String> sortedMap = new LinkedHashMap<Integer, String>();
		for (Entry<Integer, String> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	public boolean isIndexPresent(Table table, Index idx) {
		boolean r = false;
		for (Index i : table.getIndexes()) {
			if (i.getIdxName().equals(idx.getIdxName()))
				r = true;
		}
		return r;
	}

	private void dropIndex(String sql, StringTokenizer tokenizer)
			throws Exception {
		try {
			// Get Index name
			String indexName = tokenizer.nextToken();

			// System.out.println("idx = " + indexName);
			// Check for the semicolon
			String tok = tokenizer.nextToken();
			if (!tok.equals(";")) {
				throw new NoSuchElementException();
			}

			// Check if there are more tokens
			if (tokenizer.hasMoreTokens()) {
				throw new NoSuchElementException();
			}

			// Delete the table if everything is ok
			boolean isDropped = false;
			for (Table table : tables) {
				for (Index idx : table.getIndexes()) {
					if (idx.getIdxName().equalsIgnoreCase(indexName)) {
						idx.delete = true;
						isDropped = true;
						table.setNumIndexes(table.getNumIndexes() - 1);
						break;
					}
				}
			}

			if (isDropped) {
				out.println("Index " + indexName + " was dropped.");
			} else {
				out.println("Error: Index " + indexName + " does not exist.");

			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid DROP INDEX statement. '" + sql + "'.");
		}

	}

	private void runStats(String tableName, String sql,
			StringTokenizer tokenizer) throws Exception {

		try {
			if (!tokenizer.equals(";") && !tokenizer.hasMoreTokens()) {
				throw new NoSuchElementException();
			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid RUNSTATS statement. '" + sql + "'.");
		}
		// System.out.println("\nIn runstats");

		HashMap colCard;
		List<String> listHighLowStr;
		List<Integer> listHighLowInt;

		for (Table table : tables) {
			if (table.getTableName().equalsIgnoreCase(tableName)) {

				ArrayList<String> data = table.getData();

				// Table Card
				table.setTableCard(data.size());

				// Column Card
				for (Column col : table.getColumns()) {

					colCard = new HashMap();
					listHighLowStr = new ArrayList<String>();
					listHighLowInt = new ArrayList<Integer>();

					// System.out.println("\nFor col = " + col.getColName());

					for (String s : data) {
						// System.out.println("\nline = " + s);

						StringTokenizer rowToken = new StringTokenizer(s);
						String row_ref[] = new String[100];

						int k = 0;
						while (rowToken.hasMoreTokens()) {
							row_ref[k++] = rowToken.nextToken();
						}

						String value = row_ref[col.getColId()];
						// System.out.println("\nvalue = " + value);

						if (!value.isEmpty()
								&& (!(value.equals("£") || value.equals("-")))) {

							colCard.put(value, 1);

							if (col.getColType().toString().equals("CHAR"))
								listHighLowStr.add(value);
							else
								listHighLowInt.add(Integer.parseInt(value));

						}

					}
					/*
					 * System.out.println("\nsize = " + colCard.size());
					 * System.out.println("\nhighKey = " +
					 * Collections.max(listHighLow));
					 * System.out.println("\nlowkey = " +
					 * Collections.min(listHighLow));
					 */

					// SET COL-CARD
					col.setColCard(colCard.size());

					// SET HIGH-KEY LOW-KEY
					if (col.getColType().toString().equals("CHAR")) {
						col.setHiKey(Collections.max(listHighLowStr));
						col.setLoKey(Collections.min(listHighLowStr));
					} else {
						col.setHiKey((Collections.max(listHighLowInt))
								.toString());
						col.setLoKey((Collections.min(listHighLowInt))
								.toString());
					}
				}
			}
		}
	}
}
