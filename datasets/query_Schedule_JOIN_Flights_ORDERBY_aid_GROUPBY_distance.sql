SELECT Schedule.aid,Schedule.flno,Flights.flno,Flights.distance FROM Schedule,Flights
WHERE Schedule.flno=Flights.flno
GROUPBY Flights.distance
ORDERBY Schedule.aid