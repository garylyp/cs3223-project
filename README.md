# cs3223-project
In cs3223-project, we implement an SQL Query Processor. Our team consists of Gary, Kee En, and Teik Jun.
Based on a scaffold of the query processor, we have implemented join algorithms together with other relational operators. We've also fixed a limitation in the query processor.

## Join Algorithms
1. [Block-nested Join](./src/qp/operators/BlockNestedJoin.java) 
2. [Sort-merge Join](./src/qp/operators/SortMergeJoin.java) and [External Sort](./src/qp/operators/ExternalSort.java) subroutine

## Other Relational Operators
1. [OrderBy](./src/qp/operators/OrderBy.java) 
2. [Distinct](./src/qp/operators/Distinct.java) 
3. [GroupBy](./src/qp/operators/GroupBy.java) 

## Fixed Limitations
Originally, the query processor was unable to perform cross joins. We have extended the query processor to allow cross joins via [CrossProduct](./src/qp/operators/CrossProduct.java).

## Setup
To set up the project locally as a user, you can refer to the instructions found on the [cs3223 website](https://www.comp.nus.edu.sg/~tankl/cs3223/project/user.htm).
