
package uzh.tomdb.db.operations;

import java.util.List;

import uzh.tomdb.db.operations.helpers.WhereCondition;

/**
 *
 * @author Francesco Luminati
 */
public class Delete implements Operations{
    private String tabName;
    private List<WhereCondition> whereOperations;
    
    public Delete(String tabName, List<WhereCondition> whereOperations) {
        this.tabName = tabName;
        this.whereOperations = whereOperations;
    }

    @Override
    public String toString() {
        return "Delete{" + "tabName=" + tabName + ", whereOperations=" + whereOperations + '}';
    }

    @Override
    public void init() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
