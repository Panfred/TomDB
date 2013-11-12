
package uzh.tomdb.db.operations;

import java.util.List;

import uzh.tomdb.db.operations.helpers.SetCondition;
import uzh.tomdb.db.operations.helpers.WhereCondition;

/**
 *
 * @author Francesco Luminati
 */
public class Update implements Operations{
    private String tabName;
    private List<SetCondition> setOperations;
    private List<WhereCondition> whereOperations;

    public Update(String tabName, List<SetCondition> setOperations, List<WhereCondition> whereOperations) {
        this.tabName = tabName;
        this.setOperations = setOperations;
        this.whereOperations = whereOperations;
    }

    @Override
    public String toString() {
        return "Update{" + "tabName=" + tabName + ", setOperations=" + setOperations + ", whereOperations=" + whereOperations + '}';
    }

    @Override
    public void init() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
