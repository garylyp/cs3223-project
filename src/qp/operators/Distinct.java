/**
 * Page Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class Distinct extends Operator {

    static int filenum = 0;         // To get unique filenum for this operation
    String rfname;                  // The file name where the right table is materialized
    ObjectInputStream in;           // File pointer to the right hand materialized file

    final ArrayList<Attribute> attrset;
    private ArrayList<Integer> projectIndices = new ArrayList<>();
    Operator base; // the base operator
    private ExternalSort sortedBase; // the sort operator being applied on the base operator
    private int batchsize; // Number of tuples per out batch
    private int numBuff; // Number of buffers available
    private boolean eos = false; // records whether we have reached end of stream
    private Batch inBatch = null; // input batch
    private int inIndex = 0; // the index for the current element being read from input batch
    private Tuple lastOutTuple = null; // the last tuple being output
    
    public Distinct(Operator base, ArrayList<Attribute> as, int type) {
    	super(type);
    	this.base = base;
    	this.attrset = as;
    }
    
    public Distinct(Operator base, ArrayList<Attribute> as, int type, int numBuff) {
    	super(type);
    	this.base = base;
    	this.attrset = as;
    	this.numBuff = numBuff;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getAttrSet() {
        return attrset;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNumBuff() {
        return this.numBuff;
    }

    /**
     * During open
     * * Runs External Sort on base operator
     **/
    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        for (Object attribute : this.attrset) {
        	projectIndices.add(schema.indexOf( (Attribute) attribute));
        }
        sortedBase = new ExternalSort(base, attrset, OpType.DISTINCT, numBuff);
        sortedBase.setSchema(schema);
        if (!sortedBase.open()) {
            System.out.println("External sort failed to open");
            return false;
        }
        return true;
    }

    /**
     *
     **/
    public Batch next() {
        if (eos) {
        	close();
        	return null;
        } else if (inBatch == null) {
        	inBatch = sortedBase.next();
        } 
        
        Batch outBatch = new Batch(batchsize);
        lastOutTuple = null;
        while (!outBatch.isFull()) {
        	
        	// if finished scanning 
        	if (inBatch == null || inBatch.size() <= inIndex) {
        		eos = true;
        		break;
        	}
        	
        	Tuple current = inBatch.get(inIndex);
        	if (lastOutTuple == null || !isEqualTuples(lastOutTuple, current)) {
        		outBatch.add(current);
        		lastOutTuple = current;
        	}
        	inIndex++;
        	
        	if (inIndex == batchsize) {
        		inBatch = sortedBase.next();
        		inIndex = 0;
        	}
        }
        return outBatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        return sortedBase.close();
    }
    
    private boolean isEqualTuples(Tuple tuple1, Tuple tuple2) {
    	for (int index : projectIndices) {
    		if (Tuple.compareTuples(tuple1, tuple2, index) != 0) {
    			return false;
    		}
    	}
    	return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Distinct newDistinct = new Distinct(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newDistinct.setSchema(newSchema);
        return newDistinct;
    }

}
