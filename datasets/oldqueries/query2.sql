SELECT *
FROM Schedule,Certified,Employees
WHERE Schedule.aid=Certified.aid,Certified.eid=Employees.eid
