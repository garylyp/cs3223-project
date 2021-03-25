SELECT Certified.eid,Certified.aid,Schedule.aid,Schedule.flno,Flights.flno,Flights.distance 
FROM Certified,Schedule,Flights
WHERE Certified.aid=Schedule.aid,Schedule.flno=Flights.flno
