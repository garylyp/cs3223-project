/**
 * External sorts the base relational table
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * External Sort operator - sort data from a file
 */
public class ExternalSort extends Operator {
	
	static int filenum = 0;

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    ArrayList<Integer> attrIndex;    // index of the attributes in the base operator
    int numBuff;					 // Number of buffers available
    int batchsize;                 // Number of tuples per outbatch
    String rfname;				   // Name of temp storage file
    File sortedFile;
    
    ObjectInputStream sortedFileBase;
    boolean sortedFileEos;
    
    /**
     * Constructor - just save filename
     */
    public ExternalSort(Operator base, ArrayList<Attribute> as, int type, int numBuff) {
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

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }
    
    public int getNumBuff() {
    	return this.numBuff;
    }



    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * sorted on from the base operator
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
        attrIndex = new ArrayList<Integer>(attrset.size());
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex.add(index);
        }
        
        // generate sorted runs
        ArrayList<String> sortedRuns = generateSortedRuns(base);
        base.close();

        // merge sorted run
        rfname = mergeSortedRuns(sortedRuns);
        
        try {
        	sortedFileBase = new ObjectInputStream(new FileInputStream(rfname));
            sortedFileEos = false;
        } catch (IOException io) {
            System.err.println("ExternalSort: error in reading sorted file" + rfname);
            return false;
        }
        return true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        if (sortedFileEos) {
            return null;
        }
		try {
	    	Batch outbatch = (Batch) sortedFileBase.readObject();
	        return outbatch;
	        
		} catch (ClassNotFoundException cnf) {
            System.err.println("ExternalSort: Class not found for reading file  " + rfname);
            System.exit(1);
	    } catch (EOFException e) {
	    	sortedFileEos = true;
	    } catch (IOException io) {
            System.out.println("ExternalSort: " + io.getMessage());
            System.exit(1);
	    }
		return null;
    }

    /**
     * Close the file.. This routine is called when the end of filed
     * * is already reached
     **/
    public boolean close() {
    	File f = new File(rfname);
        f.delete();
        return true;
    }

    public Object clone() { 
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        ExternalSort newExternalSort = new ExternalSort(newbase, newattr, optype, numBuff);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newExternalSort.setSchema(newSchema);
        return newExternalSort;
    }
    
    
    private ArrayList<String> generateSortedRuns(Operator base) {
    	ArrayList<String> sortedRuns = new ArrayList<>();
    	Batch inbatch = base.next();
 		try {
	    	while (inbatch != null && inbatch.size() > 0) {
	    		// Initialize output file
	         	String srfname = "SRtemp-" + String.valueOf(filenum++); 
	         	ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(srfname));
	         	ArrayList<Tuple> srTuples = new ArrayList<>();
	         	
	         	// Read B pages per sorted run
	         	int numPagesRead = 0; 
	         	while (inbatch != null  && inbatch.size() > 0 && numPagesRead < numBuff) {
	             	for (int i = 0; i < inbatch.size(); i++) {
	             		srTuples.add(inbatch.get(i));
	             	}
	             	numPagesRead++;
	             	inbatch = base.next();
	         	}
	         	
	     		// In-memory sort tuples by attributes
	     		srTuples.sort((t1, t2)-> {
	     			return Tuple.compareTuples(t1, t2, attrIndex, attrIndex);
	     		});
	     		
         		// Write tuples into output file page by page to form a sorted run
	     		int cnt = 0;
         		Batch outbatch = new Batch(batchsize);
         		for (int j = 0; j < srTuples.size(); j++) {
         			outbatch.add(srTuples.get(j));
         			if (outbatch.isFull()) {
         				out.writeObject(outbatch);
         				cnt++;
         				outbatch = new Batch(batchsize);
         			}
         		}
         		System.out.println("Number of pages in sorted run: " + cnt);
         		out.close();
         		sortedRuns.add(srfname); // Store filename to output array
	    	}
	    	
 		} catch (IOException io) {
            System.out.println("ExternalSort: " + io.toString());
            System.exit(1);
        } 
 		
 		System.out.printf("ExternalSort: %d sorted runs\n", sortedRuns.size());
 		
    	return sortedRuns;
    }
    
    
    private String mergeSortedRuns(ArrayList<String> sortedRuns) {
        // Merge sorted runs. Each iteration of this while loop represent one pass
        while (sortedRuns.size() > 1) {
        	System.out.printf("Before merge pass: SortedRuns.size() = %d\n", sortedRuns.size());
        	
        	ArrayList<String> newSortedRuns = new ArrayList<>();
        	
        	int i = 0;
        	while (i < sortedRuns.size()) {
        		        		
        		// Sort B-1 sortedRuns at each pass
        		ArrayList<String> mergeSet = new ArrayList<>();
        		while (i < sortedRuns.size() && mergeSet.size() < numBuff - 1) {
            		mergeSet.add(sortedRuns.get(i++));
            	}
        		// |R| writes and |R| reads
        		String newSortedRun = multiwayMerge(mergeSet);
        		if (newSortedRun == "") {
        			System.out.println("ExternalSort: Error in multiway merge");
        			return "";
        		}
        		newSortedRuns.add(newSortedRun);
        	}

        	assert(newSortedRuns.size() == Math.ceil((double)sortedRuns.size() / (double)(numBuff - 1)));
        	
        	// Delete old files
        	for (String oldsrfname : sortedRuns) {
        		File f = new File(oldsrfname);
        		f.delete();
        	}
        	sortedRuns = newSortedRuns;
        	System.out.printf("After merge pass: SortedRuns.size() = %d\n", sortedRuns.size());
        }
    	return sortedRuns.get(0);
    }
    
    
    private String multiwayMerge(ArrayList<String> mergeSet) {
    	ArrayList<ObjectInputStream> scannedRuns = new ArrayList<>();
    	ArrayList<Boolean> eos = new ArrayList<>();  // maintain the end of stream status of every input stream
    	ArrayList<Batch> inbatches = new ArrayList<>(); // maintain the current batch pointer for each run
    	ArrayList<Integer> idxs = new ArrayList<>(); // maintain the current tuple index for each run
    	HashSet<Integer> completedRuns = new HashSet<>();
    	
    	// Scan every sorted run file 
    	for (int i = 0; i < mergeSet.size(); i++) {
    		ObjectInputStream scannedRun;
    		try {
    			scannedRun = new ObjectInputStream(new FileInputStream(mergeSet.get(i)));
    		} catch (Exception e) {
                System.err.println("External Sort: Error reading " + mergeSet.get(i) + "\n" + e.getMessage());
                return "";
            }
    		scannedRuns.add(scannedRun);
    		inbatches.add(null);
    		eos.add(false);
    		idxs.add(0);
    	}
    	
    	// Perform k way merge
    	String srfname = "SRtemp-" + String.valueOf(filenum++);
    	
    	try {
         	ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(srfname));
    	
         	Batch outbatch = new Batch(batchsize); // initialize output buffer
	     	while (completedRuns.size() < mergeSet.size()) {
	     		
	     		// Linear scan to choose minimum tuple to be added to output page
	     		Tuple t = null;
	     		int minIdx = -1;
	     		for (int i = 0; i < scannedRuns.size(); i++) {
	     			// If current sorted run is already at end of stream, continue
	     			if (eos.get(i)) {
	     				continue;
	     				
	     			// If cursor is at start of page, pull new page in
	     			} else if (idxs.get(i) == 0) {
	     				try {
	     					inbatches.set(i, (Batch)scannedRuns.get(i).readObject());
	                    } catch (EOFException e) {
	                        eos.set(i, true); // reached end of last page for i-th run
	                        completedRuns.add(i); // add i-th run to the set of completed runs
	                    } 
	     			}
	     			
	     			// get front tuple of current SortedRun's page
	 				Batch currbatch = inbatches.get(i);
	 				Tuple currtuple = currbatch.get(idxs.get(i));
	     			// get minimum tuple
	     			if (minIdx == -1) {
	     				t = currtuple;
	     				minIdx = i;
	     			} else if (currtuple.compareTuples(t, currtuple, attrIndex, attrIndex) <= 0) {
	     				// keep t if t is smaller than currtuple
	     			} else {
	     				// keep currtuple if t is larger than currtuple
	     				t = currtuple;
	     				minIdx = i;
	     			}
	     		}
	     		
	     		// Add selected minimum tuple to output page
	 			outbatch.add(t);
	 			// If output page is full, flush it to file and create a new page
	 			if (outbatch.isFull()) {
	 				try {
		 				out.writeObject(outbatch);
	 	            } catch (IOException io) {
	 	                System.out.println("ExternalSort: Error writing to temporary file");
	 	                return "";
	 	            }
	 				outbatch = new Batch(batchsize);
	 			}
	     		
	     		// increment idx for page where tuple was retrieved
	     		idxs.set(minIdx, idxs.get(minIdx)+1);
	     		
	     		// reached end of page for this sortedRun
	 			// set index to 0 so that it will retrieve the next page in next loop
	     		if (idxs.get(minIdx) >= inbatches.get(minIdx).size()) { 
	     			idxs.set(minIdx, 0);
	     		}
	     	}
	     	
     	
	     	// Close current out file
	        try {
	            out.close();
	        } catch (IOException e) {
	            System.err.println("ExternalSort: Error closing " + srfname);
	            return "";
	        }
	        
	        // Close old sorted runs file
	        try {
		    	for (ObjectInputStream runs : scannedRuns) {
		    		runs.close();
		    	}
	        } catch (IOException e) {
	            System.err.println("ExternalSort: Error closing sorted run file");
	            return "";
	        }
	    	
    	} catch (ClassNotFoundException c) {
            System.out.println("ExternalSort: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("ExternalSort: " + io.getMessage());
            System.exit(1);
        }
    	return srfname;
    }
}


