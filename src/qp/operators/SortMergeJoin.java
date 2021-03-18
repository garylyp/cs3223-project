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
    static final int DEBUGLEVEL = 3;         // Level of debug messages. 0 = no message. 3 = most verbose
    
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ExternalSort sortedLeft;
    ExternalSort sortedRight;    
    
    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    int lpageidx;					// Page index of left sorted relation
    int rpageidx;					// Page index of right sorted relation
    boolean matching;				// Indicate whether it is a state of matching or not
    boolean checkNextLeftPage;		// Indicate whether the current matches on left relation might persist to the next page
    boolean checkNextRightPage;
    
    int lcursLimit;					// Indicate the last index + 1 of left page that is a duplicate
    int rcursLimit;					// Indicate the last index + 1 of right page that is a duplicate

    int rpageidxStart;				// Indicate the first page index in right relation where a match was first found
    int rcursStart;
    int lcursStart;
    
    boolean firstCall;
    boolean isFinished;
    
    
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        if (DEBUGLEVEL>=3) Debug.PPrint(this);
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
        
        if (DEBUGLEVEL>=3) {
        	System.out.println("Printing left and right sorted files");
            int k = 0;
            leftbatch = sortedLeft.getBatch(k);
            while (leftbatch != null) {
            	Debug.PPrint(leftbatch);
            	leftbatch = sortedLeft.getBatch(++k);
            }
            k = 0;
            rightbatch = sortedRight.getBatch(k);
            while (rightbatch != null) {
            	Debug.PPrint(rightbatch);
            	rightbatch = sortedRight.getBatch(++k);
            }        	
        }


        lcurs = -1;
        rcurs = -1;
        rpageidx = 0;
        matching = false;
        checkNextLeftPage = false;
        checkNextRightPage = false;
        firstCall = true;
        isFinished = false;
        return true;
    }

    public Batch next() {
    	// Fetch new left and right page
    	if (firstCall) {
			leftbatch = sortedLeft.next();
			lcurs = 0;
			rightbatch = sortedRight.getBatch(rpageidx);
			rcurs = 0;
			firstCall = false;
		}
    	
    	if (isFinished) {
    		if (DEBUGLEVEL>=2) System.out.println("isFinished!");
    		return null;
    	}
    	
    	outbatch = new Batch(batchsize);
    	while (!outbatch.isFull() && !isFinished) {
    		if (!matching) {
    			if (DEBUGLEVEL>=2) System.out.println("NOT MATCHING");
    			int compareRes = Tuple.compareTuples(leftbatch.get(lcurs), rightbatch.get(rcurs), leftindex, rightindex);
    			
    			// Case 1: lcurs smaller than rcurs
        		if (compareRes < 0) { 
        			if (DEBUGLEVEL>=2) System.out.println("  Case1: left smaller");
        			lcurs++;
        			if (lcurs == leftbatch.size()) {
						if ((leftbatch = sortedLeft.next()) == null) {
							isFinished = true;
							if (DEBUGLEVEL>=2) System.out.println("    Case1a: fetch new LEFT and reached end");
						}
						lcurs = 0;
        			}
        			
    			// Case 2: rcurs smaller than lcurs        		
        		} else if (compareRes > 0) {
        			if (DEBUGLEVEL>=2) System.out.println("  Case2: right smaller");
        			rcurs++;
        			if (rcurs == rightbatch.size()) {
        				rpageidx++;
        				if ((rightbatch = sortedRight.getBatch(rpageidx))==null) {
        					isFinished = true;
        					if (DEBUGLEVEL>=2) System.out.println("    Case2a: fetch new RIGHT and reached end");
        				}
        				rcurs = 0;
        			}
        			
        		// Case 3: First match found
        		} else {
        			if (DEBUGLEVEL>=2) System.out.println("  Case3: new match found");
        			matching = true;
        			
        			// Find the chain of duplicate tuples in left page
        			// if (lcurs > lcursStart)
        			lcursStart = lcurs;
        			lcursLimit = lcurs;
        			while (lcursLimit < leftbatch.size()) {
        				if (Tuple.compareTuples(leftbatch.get(lcurs), leftbatch.get(lcursLimit), leftindex, leftindex) == 0) {
        					lcursLimit++;
        				} else {
        					break;
        				}
        			}
        			// Flag to indicate if the duplicates may possibly continue to next page
        			if (lcursLimit == leftbatch.size()) {
        				checkNextLeftPage = true;
        			} else {
        				checkNextLeftPage = false;
        			}
        			if (DEBUGLEVEL>=2) System.out.println("  Case3a: check Next Left Page = " + checkNextLeftPage);
        			
        			// Find the chain of duplicate tuples in right page
        			// Update start pointer on right page
        			if (!checkNextLeftPage) {
            			rpageidxStart = rpageidx;
            			rcursStart = rcurs;        				
        			}
        			rcursLimit = rcurs;
        			while (rcursLimit < rightbatch.size()) {
        				if (Tuple.compareTuples(rightbatch.get(rcurs), rightbatch.get(rcursLimit), rightindex, rightindex) == 0) {
        					rcursLimit++;
        				} else {
        					break;
        				}
        			}
        			// Flag to indicate if the duplicates may possibly continue to next page
        			if (rcursLimit == rightbatch.size()) {
        				checkNextRightPage = true;
        			} else {
        				checkNextRightPage = false;
        			}
        			if (DEBUGLEVEL>=2) System.out.println("  Case3b: check Next Right Page = " + checkNextRightPage);
        		}
    		}
    		
    		if (matching) {
    			if (DEBUGLEVEL>=2) System.out.println("MATCHING");
    			Tuple lefttuple = leftbatch.get(lcurs);;
    			Tuple righttuple = rightbatch.get(rcurs);
    			int compareRes = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
    			if (compareRes == 0) {
    				Tuple outtuple = lefttuple.joinWith(righttuple);
    				outbatch.add(outtuple);	
                    if (DEBUGLEVEL>=3) {
                    	System.out.println("out tuple");
                        Debug.PPrint(outtuple);
                    }
    			} else {
    				// Shouldn't enter here
    				matching = false;
    				continue;
    			}               
                if (lcurs == lcursLimit-1 && rcurs == rcursLimit-1) {
                	if (DEBUGLEVEL>=2) System.out.println("end of chain left and right");
                	// Case 1: Both false
                	if (!checkNextLeftPage && !checkNextRightPage) {
                		if (DEBUGLEVEL>=2) System.out.println("  Case1: false | false");
                		matching = false;
                		rcurs++;
                		checkNextLeftPage = false;
                		
                	// Case 2: left false, right true
                	} else if (!checkNextLeftPage && checkNextRightPage) {
                		if (DEBUGLEVEL>=2) System.out.println("  Case2: false | true");
                		rpageidx++;
                		rightbatch = sortedRight.getBatch(rpageidx);
                		if (rightbatch == null) {
                			if (DEBUGLEVEL>=2) System.out.println("    Case2a: Next right null");
                			matching = false;
                			isFinished = true;
                		} else if (Tuple.compareTuples(righttuple, rightbatch.get(0), rightindex, rightindex) != 0) {
                			if (DEBUGLEVEL>=2) System.out.println("    Case2b: Next right no match");
                			matching = false;
                			rcurs = 0;
                		} else {
                			if (DEBUGLEVEL>=2) System.out.println("    Case2c: Next right match");
                			matching = false;
                			lcurs = 0;
                			rcurs = 0;
                			checkNextRightPage = false;
                		}
                		
                	// Case 3: left true, right false
                	} else if (checkNextLeftPage && !checkNextRightPage) {
                		if (DEBUGLEVEL>=2) System.out.println("  Case3: true | false");
                		leftbatch = sortedLeft.next();
                		if (leftbatch == null) {
                			if (DEBUGLEVEL>=2) System.out.println("    Case3a: Next left null");
                			matching = false;
                			isFinished = true;
                		} else if (Tuple.compareTuples(lefttuple, leftbatch.get(0), leftindex, leftindex) != 0) {
                			if (DEBUGLEVEL>=2) System.out.println("    Case3b: Next left no match");
                			matching = false;
                			checkNextLeftPage = false;
                			lcurs = 0;
                		} else {
                			if (DEBUGLEVEL>=2) System.out.println("    Case3c: Next left match");
                			matching = false;
                			if (rpageidx != rpageidxStart) {
                				rpageidx = rpageidxStart;
                    			rightbatch = sortedRight.getBatch(rpageidx);	
                			}
                			rcurs = rcursStart;
                			lcurs = 0;
                		}
                		
                	// Case 4: left true, right true
                	} else {
                		if (DEBUGLEVEL>=2) System.out.println("  Case4: true | true");
                		rpageidx++;
                		rightbatch = sortedRight.getBatch(rpageidx);
                		if (rightbatch == null || Tuple.compareTuples(righttuple, rightbatch.get(0), rightindex, rightindex) != 0) {
                			if (DEBUGLEVEL>=2) System.out.println("    Case4a: Next right null or next right no match");
                    		leftbatch = sortedLeft.next();
                    		if (leftbatch == null) {
                    			if (DEBUGLEVEL>=2) System.out.println("      Case4a1: Next left null");
                    			matching = false;
                    			isFinished = true;
                    		} else if (Tuple.compareTuples(lefttuple, leftbatch.get(0), leftindex, leftindex) != 0) {
                    			if (DEBUGLEVEL>=2) System.out.println("      Case4a2: Next left no match");
                    			matching = false;
                    			lcurs = 0;
                    			rcurs = 0;
                    			checkNextLeftPage = false;
                    		} else {
                    			if (DEBUGLEVEL>=2) System.out.println("      Case4a3: Next left match");
                    			matching = false;
                    			rpageidx = rpageidxStart;
                    			rightbatch = sortedRight.getBatch(rpageidx);
                    			rcurs = rcursStart;  
                    			lcurs = 0;
                    		}           	
                			
                		} else {
                			if (DEBUGLEVEL>=2) System.out.println("    Case4b: Next right match");
                			matching = false;
                			lcurs = 0;
                			rcurs = 0;
                			checkNextRightPage = false;
                		}
                    	
                	}
                } else if (lcurs != lcursLimit-1 && rcurs == rcursLimit-1) {
                	if (DEBUGLEVEL>=2) System.out.println("end of chain right"); // restart right
                	lcurs++;
	                if (rpageidx == rpageidxStart) {
	            		rcurs=rcursStart;
	            	} else {
	            		rcurs = 0;
	            	}
                	if (DEBUGLEVEL>=2) System.out.println("end of chain left");
                } else if (lcurs == lcursLimit-1 && rcurs != rcursLimit-1) {
                	if (DEBUGLEVEL>=2) System.out.println("end of chain left");
                	rcurs++;
                } else { // (lcurs != lcursLimit-1 && rcurs != rcursLimit-1)
                	if (DEBUGLEVEL>=2) System.out.println("end of chain left");
                	rcurs++;
                }
                
                if (outbatch.isFull() || isFinished) {
                	if (DEBUGLEVEL>=3) {
                		System.out.println("Page full. Return");
                		Debug.PPrint(outbatch);
                	}
                	return outbatch;
                }
    		}    		
    	}
    	
    	if (DEBUGLEVEL>=2) System.out.println("isFinished " + isFinished);
    	if (DEBUGLEVEL>=2) System.out.println("isFull " + outbatch.isFull());
    	if (isFinished && outbatch.size() > 0) {
        	if (DEBUGLEVEL>=3) {
        		System.out.println("Page full. Return");
        		Debug.PPrint(outbatch);
        	}
    		return outbatch;
    	} else {
    		return null;
    	}
    }

    public boolean close() {
    	if (DEBUGLEVEL>=2) System.out.println("Close Sort merge join");
    	sortedLeft.close();
    	sortedRight.close();
    	return true;
    }
}
    

