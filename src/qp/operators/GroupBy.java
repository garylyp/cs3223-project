package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.ObjectInputStream;
import java.util.ArrayList;

public class GroupBy extends Operator {
    Operator base;                              // the base operator
    private ArrayList<Attribute> groupbyList;   // Set of attributes to group by
    private int batchsize;                      // Number of tuples per out batch
    private int numBuff;                        // Number of buffers available
    private boolean eos = false;                // records whether we have reached end of stream

    private ArrayList<Integer> projectIndices = new ArrayList<>();  // Set of index of the attributes in the base operator that are to be projected
    private ExternalSort sortedBase;            // the sort operator being applied on the base operator

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getGroupByList() {
        return groupbyList;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNumBuff() {
        return this.numBuff;
    }

    public GroupBy(Operator base, ArrayList<Attribute> groupbyList, int type) {
        super(type);
        this.base = base;
        this.groupbyList = groupbyList;
    }

    /**
     * During open
     * * Runs External Sort on base operator
     **/
    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        for (Object attribute : this.groupbyList) {
            projectIndices.add(schema.indexOf( (Attribute) attribute));
        }
        sortedBase = new ExternalSort(base, groupbyList, OpType.DISTINCT, numBuff);
        sortedBase.setSchema(schema);
        if (!sortedBase.open()) {
            System.out.println("External sort failed to open");
            return false;
        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        Batch inBatch = null; // input batch
        int inIndex = 0; // the index for the current element being read from input batch

        if (eos) {
            close();
            return null;
        } else if (inBatch == null) {
            inBatch = sortedBase.next();
        }

        Batch outBatch = new Batch(batchsize);

        while (!outBatch.isFull()) {

            // if finished scanning
            if (inBatch == null || inBatch.size() <= inIndex) {
                eos = true;
                break;
            }

            Tuple current = inBatch.get(inIndex);
            if (current != null)
                outBatch.add(current);
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

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < groupbyList.size(); ++i)
            newattr.add((Attribute) groupbyList.get(i).clone());
        GroupBy newGroupBy = new GroupBy(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newGroupBy.setSchema(newSchema);
        newGroupBy.setNumBuff(numBuff);
        return newGroupBy;
    }
}
