/**
 * To order the result by the given attributes
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

import java.util.ArrayList;

public class Order extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    int numBuff;                   // Number of buffer use for sorting
    boolean isDesc;                // Sort order
    boolean eos;

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    // Batch inbatch;
    // Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;
    
    ExternalSort sortedBase;

    public Order(Operator base, ArrayList<Attribute> as, int type, boolean isDesc) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.isDesc = isDesc;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getOrderAttr() {
        return attrset;
    }
    
    public int getNumBuff() {
        return numBuff;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }
    
    public boolean getIsDesc() {
        return isDesc;
    }

    public void setIsDesc(boolean isDesc) {
        this.isDesc = isDesc;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
        }

        sortedBase = new ExternalSort(base, attrset, OpType.EXTERNAL_SORT, numBuff);
        sortedBase.setSchema(baseSchema);
        sortedBase.setIsDesc(isDesc);
        if (!sortedBase.open()) {
        	return false;
        }
        eos = false;
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
    	if (eos) {
    		return null;
    	}
        Batch outbatch = sortedBase.next();
        if (outbatch == null) {
        	eos = true;
        	return null;
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        sortedBase.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Order newOrder = new Order(newbase, newattr, optype, isDesc);
        Schema newSchema = newbase.getSchema();
        newOrder.setSchema(newSchema);
        newOrder.setNumBuff(numBuff);
        return newOrder;
    }
}
