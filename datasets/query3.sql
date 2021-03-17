SELECT Test1.aid,Test2.tid 
FROM Test1,Test2
WHERE Test2.tid>="5"
GROUPBY Test2.tid
