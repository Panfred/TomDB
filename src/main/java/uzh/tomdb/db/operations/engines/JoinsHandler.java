 package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class JoinsHandler implements Handler{
	private final Logger logger = LoggerFactory.getLogger(JoinsHandler.class);  
	private Select select;
	private Set<String> futureManager = new HashSet<>();
	private List<WhereCondition> whereConditions;
	private JoinCondition joinCondition;
	private List<Select> selects = new ArrayList<>();
	private List<String> tabNames;
	private Map<String, Map<String, Integer>> invIndex = new HashMap<>();
	private Map<String, Map<Integer, Row>> rows = new HashMap<>();
	private Map<String, Integer> rowCols = new LinkedHashMap<>(); 
	private ConditionsHandler conditionsHandler;
	private Set<String> elaboratedRows = new HashSet<>();
	private Set<String> elaboratedIndex = new HashSet<>();
	private Map<String, Map<Integer, List<Integer>>> indexes = new HashMap<>();
	private Set<String> elaboratedBlocks = new HashSet<>();
	
	public JoinsHandler(Select select) throws MalformedSQLQuery {
		this.select = select;
		tabNames = select.getTabNames();
		init();
	}
	
	private void init() throws MalformedSQLQuery {
		
		whereConditions = select.getWhereConditions();
		if(whereConditions.size() > 0) {
			for (int i = 0; i < whereConditions.size(); i++) {
				WhereCondition cond = whereConditions.get(i);
				
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
					whereConditions.remove(i);
				} else {
					throw new MalformedSQLQuery("Wrong JOIN condition, just one join supported!");
				}
			}
		} else {
			throw new MalformedSQLQuery("No join condition!");
		}
		
		conditionsHandler = new ConditionsHandler(select, whereConditions);
	}
	
	public void start() throws ClassNotFoundException, IOException, MalformedSQLQuery {
		Map<Number160, Data> tabColumns = DBPeer.getTabColumns();
		Map<Number160, Data> tabRows = DBPeer.getTabRows();
		Map<Number160, Data> tabIndexes = DBPeer.getTabIndexes();
		
		for (String tabName: select.getTabNames()) {
			Number160 tabKey = Number160.createHash(tabName);
			TableColumns tc = (TableColumns) tabColumns.get(tabKey).getObject();
			TableRows tr = (TableRows) tabRows.get(tabKey).getObject();
			TableIndexes ti = (TableIndexes) tabIndexes.get(tabKey).getObject();
			selects.add(new Select(tabName, tr, ti, tc));
			
			invIndex.put(tabName, new ConcurrentHashMap<String, Integer>());
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
	
	@Override
	public void filterRows(Map<Number160, Data> dataMap, String future) throws ClassNotFoundException, IOException, MalformedSQLQuery {

		for (Map.Entry<Number160, Data> entry : dataMap.entrySet()) {
			
			Row row = (Row) entry.getValue().getObject();
			
			//Skip empty rows!
			if(row.getRowID() >= 0) {
				invIndex.get(row.getTabName()).put(row.getCol(joinCondition.getColumn(row.getTabName())), row.getRowID());
				rows.get(row.getTabName()).put(row.getRowID(), row);
			}	
			
		}

		removeFromFutureManager(future);
		
		checkMatches();
		
		/**
		 * Poison the blocking queue.
		 */
		if (futureManager.isEmpty()) {
			select.addToResultSet(new Row(-1));
		}
	}
	
	private void checkMatches() throws MalformedSQLQuery {
		String tabOne = tabNames.get(0);
		String tabTwo = tabNames.get(1);
		
		for (Map.Entry<String, Integer> entry: invIndex.get(tabOne).entrySet()) {
			if (invIndex.get(tabTwo).containsKey(entry.getKey())) {
				joinRows(rows.get(tabOne).get(entry.getValue()), rows.get(tabTwo).get(invIndex.get(tabTwo).get(entry.getKey())));
				rows.get(tabOne).remove(entry.getKey());
				rows.get(tabTwo).remove(entry.getKey());
			}
		}
		
	}
	
	public void filterIndex(Map<Number160, Data> dataMap, String tabName, String future) throws ClassNotFoundException, IOException {
		
		for (Map.Entry<Number160, Data> entry : dataMap.entrySet()) {
			if (!elaboratedIndex.contains(tabName+":"+entry.getKey())) {
				elaboratedIndex.add(tabName+":"+entry.getKey());
				IndexedValue iv = (IndexedValue) entry.getValue().getObject();
				indexes.get(tabName).put(iv.getIndexedVal(), iv.getRowIds());
			}
		}
		
		removeFromFutureManager(future);
		
		checkIndexesMatches();
	}
	
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
	
	private void tableScans(List<Integer> tabOneRowIds, List<Integer> tabTwoRowIds) {
		List<Block> blocks = new ArrayList<>();
		
		for (Integer id: tabOneRowIds) {
			List<Block> ls = Utils.getBlocks(id, id, selects.get(0).getTr().getRowsNum(), selects.get(0).getTr().getBlockSize(), selects.get(0).getTabName());
			Block block = ls.get(0);
			if (!elaboratedBlocks.contains(block.toString())) {
				elaboratedBlocks.add(block.toString());
				blocks.add(block);
			}
		}
		for (Integer id: tabTwoRowIds) {
			List<Block> ls = Utils.getBlocks(id, id, selects.get(1).getTr().getRowsNum(), selects.get(1).getTr().getBlockSize(), selects.get(1).getTabName());
			Block block = ls.get(0);
			if (!elaboratedBlocks.contains(block.toString())) {
				elaboratedBlocks.add(block.toString());
				blocks.add(block);
			}
		}
		
		for(Block ii: blocks) {
			logger.debug("BLOCK: "+ii.toString());
		}
		
		for (Block block : blocks) {
			
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
				}	
			});
		}

	}

	private void joinRows(Row rowOne, Row rowTwo) throws NumberFormatException, MalformedSQLQuery {
		
		List<String> row = new ArrayList<>();
		row.addAll(rowOne.getRow());
		row.addAll(rowTwo.getRow());
		Row joinRow = new Row("join", 0, row, rowCols);
		
		if(!elaboratedRows.contains(joinRow.toString())) {
			conditionsHandler.filterJoinedRow(joinRow);
			elaboratedRows.add(joinRow.toString());
		}
		
	}

	@Override
	public void addToFutureManager(String future) {
		futureManager.add(future);
	}

	@Override
	public void removeFromFutureManager(String future) {
		futureManager.remove(future);
	}
	
	private String[] dotSplitString(String string) {
		return string.split("\\.");
	}
	
}
