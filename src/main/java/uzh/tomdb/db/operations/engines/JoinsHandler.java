 
package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Statement;
import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.db.indexes.IndexedValue;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.JoinCondition;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.p2p.DBPeer;
import uzh.tomdb.parser.MalformedSQLQuery;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

/**
 * 
 * Handles the joins filtering the rows that do not join on the given column.
 * 
 * @author Francesco Luminati
 */
public class JoinsHandler implements Handler{
	private final Logger logger = LoggerFactory.getLogger(JoinsHandler.class);  
	/**
	 * Select object to get MetaData information.
	 */
	private Select select;
	/**
	 * To handle asynchronous DHT operations.
	 */
	private Set<String> futureManager = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	/**
	 * Parsed where conditions, join conditions removed.
	 */
	private List<WhereCondition> whereConditions;
	/**
	 * Parsed join conditions, extracted from where conditions.
	 */
	private JoinCondition joinCondition;
	/**
	 * Select objects with the MetaData of the two tables involved in the join.
	 */
	private List<Select> selects = new ArrayList<>();
	/**
	 * Both tables name to join.
	 */
	private List<String> tabNames;
	/**
	 * The external map is used to separate table one and table two.
	 * The internal map contains as key the value of the joined column, used to find join-matches. The value refer to the corresponding row IDs that have to be joined.
	 */
	private Map<String, Map<String, List<Integer>>> invIndex = new HashMap<>();
	/**
	 * The external map is used to separate table one and table two.
	 * The internal map contains the actual row indexed in the invIndex.
	 */
	private Map<String, Map<Integer, Row>> rows = new HashMap<>();
	/**
	 * Columns of the joined row.
	 */
	private Map<String, Integer> rowCols = new LinkedHashMap<>();
	/**
	 * ConditionsHandler to further elaborate the joined row matching the where conditions.
	 */
	private ConditionsHandler conditionsHandler;
	/**
	 * Save the elaborated joined rows to avoid sending duplicates to the ResultSet.
	 */
	private Set<String> elaboratedRows = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	/**
	 * Save the elaborated indexed values in case that the DST block is full and it needs to retrieve more blocks (i.e. duplicates indexed values).
	 */
	private Set<String> elaboratedIndex = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	/**
	 * The external map is used to separate table one and table two.
	 * The internal map contains as key the indexed value, used to find join-matches. The value refer to the corresponding row IDs that have to be joined.
	 */
	private Map<String, Map<Integer, List<Integer>>> indexes = new HashMap<>();
	/**
	 * Save the elaborated table Blocks to avoid duplicate gets.
	 */
	private Set<String> elaboratedBlocks = new HashSet<>();
	private int expHash;
	
	public JoinsHandler(Select select) throws MalformedSQLQuery {
		this.select = select;
		tabNames = select.getTabNames();
		this.expHash = select.hashCode();
		init();
	}
	
	/**
	 * Internal parser to create one JoinCondition object out of the where conditions. A where-join conditions has the form of: tablename:columnname = tablename2:columnname2.
	 * At the end, a ConditionsHandler with the remaining where conditions is created.
	 */
	private void init() throws MalformedSQLQuery {
		
		whereConditions = select.getWhereConditions();
		if (whereConditions.size() > 0) {
			
				WhereCondition cond = whereConditions.get(0);
				
				if (cond.isJoinCondition()) {
					JoinCondition jc = new JoinCondition();
					String[] one = dotSplitString(cond.getColumn());
					if (select.getTabNames().contains(one[0])) {
						jc.setTabOne(one[0]);
					} else {
						throw new MalformedSQLQuery("Wrong JOIN condition");
					}
					jc.setColumnOne(one[1]);
					String[] two = dotSplitString(cond.getValue());
					if (select.getTabNames().contains(two[0])) {
						jc.setTabTwo(two[0]);
					} else {
						throw new MalformedSQLQuery("Wrong JOIN condition");
					}
					jc.setColumnTwo(two[1]);
					joinCondition = jc;
					whereConditions.remove(0);
				} else {
					throw new MalformedSQLQuery("Wrong JOIN condition!");
				}
			
		} else {
			throw new MalformedSQLQuery("No join condition!");
		}
		
		conditionsHandler = new ConditionsHandler(select, whereConditions);
	}
	
	/**
	 * First it creates a Select object for every table and add the tables to invInde, rows, and indexes maps.
	 * Then the columns of both tables are joined in a single row and they are set in the result set.
	 * At the end, two tablescans or two joinindexscans are executed, referring back to this object. 
	 */
	public void start() throws ClassNotFoundException, IOException, MalformedSQLQuery {
		Map<Number160, Data> tabColumns = DBPeer.getTabColumns();
		Map<Number160, Data> tabRows = DBPeer.getTabRows();
		Map<Number160, Data> tabIndexes = DBPeer.getTabIndexes();
		
		logger.trace("JOIN-WHOLE", "BEGIN", Statement.experiment, expHash);
		
		for (String tabName: select.getTabNames()) {
			Number160 tabKey = Number160.createHash(tabName);
			TableColumns tc = (TableColumns) tabColumns.get(tabKey).getObject();
			TableRows tr = (TableRows) tabRows.get(tabKey).getObject();
			TableIndexes ti = (TableIndexes) tabIndexes.get(tabKey).getObject();
			selects.add(new Select(tabName, tr, ti, tc));
			
			invIndex.put(tabName, new ConcurrentHashMap<String, List<Integer>>());
			rows.put(tabName, new ConcurrentHashMap<Integer, Row>());
			indexes.put(tabName, new ConcurrentHashMap<Integer, List<Integer>>());
		}
		
		//Set columns of joined row
		int counter = 0;
		for (Select select: selects) {
			for (String name: select.getTc().getColumns().keySet()) {
				rowCols.put(name, counter++);
			}
		}
		//Set columns in result set!!!
		if (select.isAllColumns()) {
			select.setResultSetColumns(rowCols);
		} else {
			Map<String, Integer> cols = new LinkedHashMap<>();
			for (int i = 0; i < select.getColumns().size(); i++) {
				cols.put(select.getColumns().get(i), i);
			}
			select.setResultSetColumns(cols);
		}
		
		//execute scans
		switch (select.getScanType()) {
		case "tablescan":
			for (Select select: selects) {
				new TableScan(select, this).start();
			}
			break;
		case "indexscan":
			for (Select select: selects) {
				new JoinIndexScan(select, this, joinCondition).start();
			}
			break;
		}
	}
	
	/**
	 * The rows asynchronous coming from the tablescans of both tables are added here to the respective invIndex and rows maps.
	 */
	@Override
	public void filterRows(Map<Number160, Data> dataMap, String future) throws ClassNotFoundException, IOException, MalformedSQLQuery {

		for (Map.Entry<Number160, Data> entry : dataMap.entrySet()) {
			
			Row row = (Row) entry.getValue().getObject();
			
		/**
		 * Skip empty rows of DELETE operations.
		 */
		if (row.getRowID() >= 0) {
			
			if (!invIndex.get(row.getTabName()).containsKey(row.getCol(joinCondition.getColumn(row.getTabName())))) {
					List<Integer> rowIds = new ArrayList<>();
					rowIds.add(row.getRowID());
					invIndex.get(row.getTabName()).put(row.getCol(joinCondition.getColumn(row.getTabName())), rowIds);
				} else {
					List<Integer> rowIds = invIndex.get(row.getTabName()).get(row.getCol(joinCondition.getColumn(row.getTabName())));
					rowIds.add(row.getRowID());
					invIndex.get(row.getTabName()).put(row.getCol(joinCondition.getColumn(row.getTabName())), rowIds);
				}
				rows.get(row.getTabName()).put(row.getRowID(), row);
			}	
			
		}
		
		removeFromFutureManager(future);	
		
	}
	
	/**
	 * For every keys of the invIndex for table one, a matching key in invIndex for table two is searched.
	 * If a match is found, the row IDs for table one and table two are sent to joinRows method.
	 */
	private void checkMatches() {
		
		String tabOne = tabNames.get(0);
		String tabTwo = tabNames.get(1);
		
		for (Map.Entry<String, List<Integer>> entry: invIndex.get(tabOne).entrySet()) {
			if (invIndex.get(tabTwo).containsKey(entry.getKey())) {
				joinRows(entry.getValue(), invIndex.get(tabTwo).get(entry.getKey()));
				invIndex.get(tabOne).remove(entry.getKey());
				invIndex.get(tabTwo).remove(entry.getKey());
			}
		}
		
		/**
		 * Poison the blocking queue.
		 */
		logger.trace("JOIN-WHOLE", "END", Statement.experiment, expHash);
		select.addToResultSet(new Row(-1));
		
	}
	
	/**
	 * For every row IDs of table one, a new joined row for every row IDs of table two is created and sent to ResultSet.
	 * 
	 * @param rowOne
	 * @param rowTwo
	 */
	private void joinRows(List<Integer> oneIds, List<Integer> twoIds) {
		String tabOne = tabNames.get(0);
		String tabTwo = tabNames.get(1);
		
		for (Integer oneId: oneIds) {
			for (Integer twoId: twoIds) {
				List<String> row = new ArrayList<>();
				row.addAll(rows.get(tabOne).get(oneId).getRow());
				row.addAll(rows.get(tabTwo).get(twoId).getRow());
				Row joinRow = new Row("join", 0, row, rowCols);
				
				if(!elaboratedRows.contains(joinRow.toString())) {
					elaboratedRows.add(joinRow.toString());
					conditionsHandler.filterJoinedRow(joinRow);
				}
			}	
		}
		for (Integer oneId: oneIds) {
			rows.get(tabOne).remove(oneId);
		}
		for (Integer twoId: twoIds) {
			rows.get(tabTwo).remove(twoId);
		}

	}
	
	/**
	 * The row IDs asynchronous coming from the joinindexscans of both tables are added here to the respective indexes map.
	 * After that the checkIndexesMatches method is started, so that every time the already possible matches over indexed values are sent forward to the tablescan.
	 */
	public void filterIndex(Map<Number160, Data> dataMap, String tabName, String future) throws ClassNotFoundException, IOException {
		
		for (Map.Entry<Number160, Data> entry : dataMap.entrySet()) {
			if (!elaboratedIndex.contains(tabName+":"+entry.getKey())) {
				elaboratedIndex.add(tabName+":"+entry.getKey());
				IndexedValue iv;	
					iv = (IndexedValue) entry.getValue().getObject();	
					indexes.get(tabName).put(iv.getIndexedVal(), iv.getRowIds());
			}
		}
		
		removeFromFutureManager(future);
		
		checkIndexesMatches();
	}
	
	/**
	 * For every keys of the indexes map for table one, a matching key in indexes map for table two is searched.
	 * If a match is found, the row IDs for table one and table two are sent to tablescan method to be retrieved from the table DHT.
	 */
	private void checkIndexesMatches() {
		String tabOne = tabNames.get(0);
		String tabTwo = tabNames.get(1);
		
		for (Map.Entry<Integer, List<Integer>> entry: indexes.get(tabOne).entrySet()) {
			if (indexes.get(tabTwo).containsKey(entry.getKey())) {
				tableScans(entry.getValue(), indexes.get(tabTwo).get(entry.getKey()));
				indexes.get(tabOne).remove(entry.getKey());
				indexes.get(tabTwo).remove(entry.getKey());
			}
		}
	}
	
	/**
	 * For every row IDs coming from the checkIndexMatches, the corresponding table block is identified. 
	 * When the block has not been retrieved yet, a DHT get operation is started. The operation returns the results back to this object to be elaborated by the filterRows method.
	 * 
	 * @param tabOneRowIds
	 * @param tabTwoRowIds
	 */
	private void tableScans(final List<Integer> tabOneRowIds, final List<Integer> tabTwoRowIds) {
		List<Block> blocks = new ArrayList<>();
		
		if (tabOneRowIds != null) {
			for (Integer id: tabOneRowIds) {
				List<Block> ls = Utils.getBlocks(id, id, selects.get(0).getTr().getRowsNum(), selects.get(0).getTr().getBlockSize(), selects.get(0).getTabName());
				Block block = ls.get(0);
				if (!elaboratedBlocks.contains(block.toString())) {
					elaboratedBlocks.add(block.toString());
					blocks.add(block);
				}
			}
		}
		
		if (tabTwoRowIds != null) {
			for (Integer id: tabTwoRowIds) {
				List<Block> ls = Utils.getBlocks(id, id, selects.get(1).getTr().getRowsNum(), selects.get(1).getTr().getBlockSize(), selects.get(1).getTabName());
				Block block = ls.get(0);
				if (!elaboratedBlocks.contains(block.toString())) {
					elaboratedBlocks.add(block.toString());
					blocks.add(block);
				}
			}
		}

		final AtomicInteger counter = new AtomicInteger(blocks.size());
		if (counter.get() > 0) {
			logger.trace("JOIN-GET-TABLE", "BEGIN", Statement.experiment, expHash, tabOneRowIds.hashCode()+tabTwoRowIds.hashCode(), counter.get());
		}
		
		for (Block block: blocks) {
			
			FutureDHT future = select.getPeer().get(block.getHash()).setAll().start();
			addToFutureManager(future.toString());
			future.addListener(new BaseFutureAdapter<FutureDHT>() {
				@Override
				public void operationComplete(FutureDHT future) throws Exception {
					if (future.isSuccess()) {
						logger.debug("GET from Table: Get succeed!");
						filterRows(future.getDataMap(), future.toString());
					} else {
						// add exception?
						logger.debug("GET from Table: Get failed!");
						removeFromFutureManager(future.toString());
					}
					if (counter.decrementAndGet() == 0) {
						logger.trace("JOIN-GET-TABLE", "END", Statement.experiment, expHash, tabOneRowIds.hashCode()+tabTwoRowIds.hashCode());	
					}
				}
			});
		
		}

	}

	@Override
	public void addToFutureManager(String future) {
		futureManager.add(future);
	}

	/**
	 * The checkMatches() is started at the end only when all the results arrived.
	 */
	@Override
	public void removeFromFutureManager(String future) {
		futureManager.remove(future);
		/**
		 * Start matching only when all rows are returned.
		 */
		if (futureManager.isEmpty()) {
			checkMatches();
		}
	}
	
	private String[] dotSplitString(String string) {
		return string.split("\\.");
	}
	
}
