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
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached



    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        
        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        
        // External Sort
        // get filename of sorted left
        // get filename of sorted right
        
    }

    public Batch next() {

    }

    public boolean close() {
    }
    
    public static String externalSort(Operator in, ArrayList<Integer> attbIndex, int numBuff, int batchSize) {
   	
    	// Generate Sorted Runs (array list of ObjectInputStream)
    	ArrayList<String> sortedRuns = new ArrayList<>();
        try {
            int numPages = 0;
        	filenum++;
        	String srfname = "SRtemp-" + String.valueOf(filenum); 
        	ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(srfname));
        	ArrayList<Tuple> srTuples = new ArrayList<>();
  
            Batch inPage;
            while ((inPage = in.next()) != null || numPages < numBuff) {
            	for (int i = 0; i < inPage.size(); i++) {
            		srTuples.add(inPage.get(i));
            	}
            	numPages++;
            	
            	// All buffer occupied
            	if (numPages == numBuff || inPage == null) {
            		// Sort arrayList by attributes
            		srTuples.sort((t1, t2)-> {
            			return Tuple.compareTuples(t1, t2, attbIndex, attbIndex);
            		});
            		// Write tuples into sorted run page by page
            		Batch outPage = new Batch(batchSize);
            		for (int j = 0; j < srTuples.size(); j++) {
            			outPage.add(srTuples.get(j));
            			if (outPage.isFull()) {
            				out.writeObject(outPage);
            				outPage = new Batch(batchSize);
            			}
            		}
            		out.close();
            		sortedRuns.add(srfname);
            		
            		if (inPage == null) {
            			break;
            		}
            		
            		// Start a new sorted run
            		numPages = 0;
                	filenum++;
                	srfname = "SRtemp-" + String.valueOf(filenum); 
                    out = new ObjectOutputStream(new FileOutputStream(srfname));
                    srTuples = new ArrayList<>();
            	}
            }
        } catch (IOException io) {
            System.out.println("External Sort: Error");
            return "";
        } 
        
        
        // Perform k-way merge of k sorted runs until only 1 sorted run remains
        while (sortedRuns.size() > 1) {
        	ArrayList<String> newSortedRuns = new ArrayList<>();
        	int i = 0;
        	ArrayList<String> mergeSet = new ArrayList<>();
        	while (i < sortedRuns.size()) {
        		// can read up to numBuff - 1 number of runs per merge
        		mergeSet.add(sortedRuns.get(i));
        		if (mergeSet.size() == numBuff - 1 || i == sortedRuns.size()-1) {
        			String newSortedRun = multiwayMerge(mergeSet, numBuff, batchSize);
        			newSortedRuns.add(newSortedRun);
        			mergeSet = new ArrayList<>();
        		}
        		
        	}
        	assert(newSortedRuns.size() == Math.ceil(sortedRuns.size() / (numBuff - 1)));
        	sortedRuns = newSortedRuns;
        }
    	
    	return sortedRuns.get(0);
    }

}

