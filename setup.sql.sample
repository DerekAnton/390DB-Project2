-- CMPSCI 390DB - Sample CUSTOMER Database schema - setup.sql
drop table rentalplans cascade;
drop table customers cascade;
drop table movierentals cascade;

CREATE TABLE RentalPlans(
  pid integer PRIMARY KEY,
  name VARCHAR(50) UNIQUE NOT NULL,
  max_movies int NOT NULL,
  fee numeric(6,2) NOT NULL
);

CREATE TABLE Customers(
  cid integer PRIMARY KEY,
  login VARCHAR(50),
  password VARCHAR(50),
  fname VARCHAR(50),
  lname VARCHAR(50),
  pid integer REFERENCES RentalPlans (pid)
);


CREATE TABLE MovieRentals(
  mid integer NOT NULL,
  cid integer REFERENCES Customers(cid),
  status VARCHAR(10) CHECK (status = 'open' or status = 'closed')
);


INSERT INTO RentalPlans VALUES (1, 'basic', 1, 1.99);
INSERT INTO RentalPlans VALUES (2, 'rental plus', 3, 2.99);
INSERT INTO RentalPlans VALUES (3, 'super access', 5, 3.99);
INSERT INTO RentalPlans VALUES (4, 'prime', 10, 4.99);

INSERT INTO Customers VALUES (1, 'george', '123', 'George', 'Ford', 1);
INSERT INTO Customers VALUES (2, 'tim', 'secret', 'Tim', 'Johnson', 1);

INSERT INTO MovieRentals VALUES(22592, 1, 'open');
INSERT INTO MovieRentals VALUES(22591, 1, 'closed');
