
package uzh.tomdb.db.operations.helpers;

import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.Tokens;

/**
 * 
 * Helper class for Join Conditions.
 * 
 * @author Francesco Luminati
 */
public class JoinCondition implements Conditions{
	
	private String tabOne;
	private String columnOne;
	private String tabTwo;
	private String columnTwo;

	public String getTabOne() {
		return tabOne;
	}

	public void setTabOne(String tabOne) {
		this.tabOne = tabOne;
	}

	public String getColumnOne() {
		return columnOne;
	}

	public void setColumnOne(String columnOne) {
		this.columnOne = columnOne;
	}

	public String getTabTwo() {
		return tabTwo;
	}

	public void setTabTwo(String tabTwo) {
		this.tabTwo = tabTwo;
	}

	public String getColumnTwo() {
		return columnTwo;
	}

	public void setColumnTwo(String columnTwo) {
		this.columnTwo = columnTwo;
	}

	public String getColumn(String tabName) throws MalformedSQLQuery {
		if (tabOne.equals(tabName)) {
			return columnOne;
		} else if (tabTwo.equals(tabName)) {
			return columnTwo;
		} else {
			throw new MalformedSQLQuery("Wrong Table Name");
		}
	}

	@Override
	public String getType() {
		return Tokens.JOIN;
	}

	@Override
	public String toString() {
		return "JoinCondition [tabOne=" + tabOne + ", columnOne=" + columnOne
				+ ", tabTwo=" + tabTwo + ", columnTwo=" + columnTwo + "]";
	}

}
