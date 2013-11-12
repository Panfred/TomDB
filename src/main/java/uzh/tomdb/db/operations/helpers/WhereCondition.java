
package uzh.tomdb.db.operations.helpers;

import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.Tokens;

/**
 *
 * @author Francesco Luminati
 */
public class WhereCondition implements Conditions{
    private String column = "";
    private String operator = "";
    private String value = "";
    private String boolOperator = "";

    
    public String getColumn() {
        return column;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }
    
    public String getBoolOperator() {
    	return boolOperator;
    }
    
    public void setOperator(String operator) {
        this.operator = operator;
    }

    public void setValue(String value) {
        this.value = value;
    }
    
    public void setColumn(String column) {
        this.column = column;
    }

    public void setBoolOperator(String bool) {
    	this.boolOperator = bool;
    }
    
    public void setColOrVal(String val) throws MalformedSQLQuery {
        if (column.isEmpty()) {
            column = val;
        } else if (value.isEmpty()) {
            value = val;
        } else {
            throw new MalformedSQLQuery("WhereOperation ERROR: both column and value are full!");
        }
    }
    
    public boolean isSet() throws MalformedSQLQuery {
        if (column.isEmpty() || value.isEmpty() || operator.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "column=" + column + ", operator=" + operator + ", value=" + value;
    }
    
    @Override
    public String getType() {
		return Tokens.WHERE;
	}
    
}
