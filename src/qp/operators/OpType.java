/**
 * enumeration of different operator types
 **/

package qp.operators;

public class OpType {

    public static final int SCAN = 0;
    public static final int SELECT = 1;
    public static final int PROJECT = 2;
    public static final int JOIN = 3;
    public static final int EXTERNAL_SORT = 4;
    public static final int GROUPBY = 5;
    public static final int ORDER = 6;
    public static final int DISTINCT = 7;
}
