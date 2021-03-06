/**
 * Sort Merge Join algorithm
 **/
package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class SortMergeJoin extends Join {
    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;					// Whether end of stream (left table) is reached
    boolean eosr;					// Whether end of stream (right table) is reached
    Operator sortedLeft;
    Operator sortedRight;    

    
    
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        Debug.PPrint(this);
    }

    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        
        /** find indices attributes of join conditions **/
        ArrayList<Attribute> leftAttrs = new ArrayList<>();
        ArrayList<Attribute> rightAttrs = new ArrayList<>();
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
            leftAttrs.add(leftattr);
            rightAttrs.add(rightattr);
        }
        
        // External Sort
        sortedLeft = new ExternalSort(left, leftAttrs, OpType.EXTERNAL_SORT, numBuff);
        sortedLeft.setSchema(left.getSchema());
        
        sortedRight = new ExternalSort(right, rightAttrs, OpType.EXTERNAL_SORT, numBuff);
        sortedRight.setSchema(right.getSchema());
        
        if (!(sortedLeft.open() && sortedRight.open())) {
        	return false;
        }

        lcurs = 0;
        rcurs = 0;
        
        leftbatch = sortedLeft.next();
        int j = 0;
        while (leftbatch != null) {
        	System.out.printf("%d\n", j++);
        	Debug.PPrint(leftbatch);
        	leftbatch = sortedLeft.next();
        }
        
        rightbatch = sortedRight.next();
        j = 0;
        while (rightbatch != null) {
        	System.out.printf("%d\n", j++);
        	Debug.PPrint(rightbatch);
        	rightbatch = sortedRight.next();
        }
        
        
        return true;
    }

    public Batch next() {
    	
    	outbatch = new Batch(batchsize);
    	//while (!outbatch.isFull()) {
    		// Fetch new left page
    		// Fetch new right page
    		
    		// Case 1
    		// lcurs smaller than rcurs | not end of left page
    		
    		// Case 2
    		// lcurs smaller than rcurs | end of left page
    		
    		// Case 3
    		// rcurs smaller than lcurs | not end of right page
    		
    		// Case 4
    		// rcurs smaller than lcurs | end of right page
    		
    		// Case 5
    		// lcurs matches rcurs | not end of right page to end of right page
    		
    		// Case 5
    		// lcurs matches rcurs | end of right page
    		
    		    		
    		
    		
    		
    	//}
    	
    	
    	return outbatch;
    }

    public boolean close() {
    	System.out.println("Close Sort merge join");
    	sortedLeft.close();
    	sortedRight.close();
    	return true;
    }
}
    

