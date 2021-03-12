/**
 * Block nested join algorithm
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.Condition;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {
    static int fileNum = 0; // To get unique filenum for this operation
    int batchSize;          // Number of tuples per out batch

    // Buffers for reading left table.
    Batch[] leftBuffers;
    // One buffer for reading right table
    Batch rightBuffer;
    // One output buffer.
    Batch outputBuffer;

    int lcurs;      // Cursor for left side buffer
    int rcurs;      // Cursor for right side buffer
    boolean eosl;   // Whether end of stream (left table) is reached
    boolean eosr;   // Whether end of stream (right table) is reached

    String rightFileName; // The file name where the right table is materialized
    ObjectInputStream in; // File pointer to the right hand materialized file

    // Indices corresponding to join attributes in left table
    ArrayList<Integer> leftIndices = new ArrayList<>();
    // Indices corresponding to join attributes in right table
    ArrayList<Integer> rightIndices = new ArrayList<>();

    /**
     * Instantiates a new join operator using block-based nested loop algorithm.
     *
     * @param jn is the base join operator.
     */
    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open, find the index of the join attributes
     * Materializes the right hand side into a file
     * Opens the connections
     **/
    @Override
    public boolean open() {
        batchSize = Batch.getPageSize() / schema.getTupleSize();

        // Obtain indices of join attributes from left and right table separately
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            leftIndices.add(left.getSchema().indexOf(leftattr));
            Attribute rightattr = (Attribute) con.getRhs();
            rightIndices.add(right.getSchema().indexOf(rightattr));
        }

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;
        Batch rightPage = null;

        if (!right.open()) {
            return false;
        } else {
            fileNum++;
            rightFileName = "BNJtemp-" + fileNum;
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightFileName));
                while ((rightPage = right.next()) != null) {
                    out.writeObject(rightPage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("Block Nested Join: Error writing to temporary file");
                return false;
            }

            if (!right.close()) {
                return false;
            }

        }
        return left.open();
    }

    /**
     * From input buffers, select the tuples satisfying join condition
     * and return a page of output tuples
     **/
    @Override
    public Batch next() {
        // Returns empty if the left table reaches end-of-stream.
        if (eosl) {
            close();
            return null;
        }

        outputBuffer = new Batch(batchSize);
        while (!outputBuffer.isFull()) {
            if (lcurs == 0 && eosr == true) {
                // Read new block from left table
                leftBuffers = new Batch[numBuff - 2];
                leftBuffers[0] = left.next();
                if (leftBuffers[0] == null) {
                    eosl = true;
                    return outputBuffer;
                }
                for (int i = 0; i < leftBuffers.length - 1; i++) {
                    leftBuffers[i + 1] = left.next();
                    if (leftBuffers[i + 1] == null) {
                        break;
                    }
                }

                /** Whenever a new left page comes, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rightFileName));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("Block Nested Join : error in reading file");
                    System.exit(1);
                }
            }

            int leftTupleCount = leftBuffers[0].size();
            for (int i = 0; i < leftBuffers.length - 1 ; i++) {
                if (leftBuffers[i + 1] == null) {
                    break;
                }
                leftTupleCount = leftBuffers[i].size() + leftTupleCount;
            }

            while (eosr == false) {
                try {
                    if (lcurs == 0 && rcurs == 0) {
                        rightBuffer = (Batch) in.readObject();
                    }

                    for (int i = lcurs; i < leftTupleCount; i++) {
                        int leftBufferSize = leftBuffers[0].size();
                        Tuple leftTuple = leftBuffers[i / leftBufferSize].get(i % leftBufferSize);

                        for (int j = rcurs; j < rightBuffer.size(); j++) {
                            Tuple rightTuple = rightBuffer.get(j);

                            if (leftTuple.checkJoin(rightTuple, leftIndices, rightIndices)) {
                                // Add the tuple
                                Tuple outputTuple = leftTuple.joinWith(rightTuple);
                                outputBuffer.add(outputTuple);

                                if (outputBuffer.isFull()) {
                                    if (i == leftTupleCount - 1 && j == rightBuffer.size() - 1) {
                                        // case 1
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != leftTupleCount - 1 && j == rightBuffer.size() - 1) {
                                        // case 2
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else {
                                        // case 3
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outputBuffer;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("Block Nested Join: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("Block Nested Join: Error in deserialising temporary file");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("Block Nested Join: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outputBuffer;
    }

    /**
     * Close the operator
     */
    @Override
    public boolean close() {
        File f = new File(rightFileName);
        f.delete();
        return true;
    }
}