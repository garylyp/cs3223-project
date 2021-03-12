package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

import java.util.ArrayList;

public class GroupBy extends Distinct {

    public GroupBy(Operator base, ArrayList<Attribute> groupbyList, int type) {
        super(base, groupbyList, type);
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        GroupBy newGroupBy = new GroupBy(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newGroupBy.setSchema(newSchema);
        return newGroupBy;
    }
}
