
package uzh.tomdb.db.operations.helpers;

import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.Tokens;

/**
 *
 * @author Francesco Luminati
 */
public class SetCondition {
    public String column = "";
    public String operator = Tokens.EQUAL;
    public String value = "";
    
    
//    public WhereOperation(String column, String operator, String value) {
//        this.column = column;
//        this.operator = operator;
//        this.value = value;
//    }
    
    public String getColumn() {
        return column;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
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
    
    public void setColOrVal(String val) throws MalformedSQLQuery {
        if (column.isEmpty()) {
            column = val;
        } else if (value.isEmpty()) {
            value = val;
        } else {
            throw new MalformedSQLQuery("SetOperation ERROR: both column and value are full!");
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


    
}
