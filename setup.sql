/* movie may not be needed 1/2
CREATE TABLE movie (
   movie_id serial,
   name text,
   year integer
);
*/

CREATE TABLE activerental (
    rental_id serial,
    movie_id integer NOT NULL,
    cust_id integer,
    dateout timestamp
);

CREATE TABLE inactiverental (
    rental_id serial,
    movie_id integer NOT NULL,
    cust_id integer,
    dateout timestamp,
    datein timestamp
);

CREATE TABLE customer (
    cust_id serial,
    username text,
    password text,
    fname text,
    lname text,
    phone text,
    plan_id integer,
    address_id integer
);

CREATE TABLE rentalplan (
    plan_id serial,
    name text UNIQUE NOT NULL,
    maxrentals integer NOT NULL, -- restriction 2a
    fee numeric(6,2) NOT NULL
);

CREATE TABLE address (
    address_id serial,
    street text,
    city text,
    state text,
    zip text
);

/* movie may not be needed 2/2
ALTER TABLE movie
    ADD CONSTRAINT movie_pkey PRIMARY KEY (movie_id);
*/

ALTER TABLE rentalplan
    ADD CONSTRAINT rentalplan_pkey PRIMARY KEY (plan_id);

ALTER TABLE address
    ADD CONSTRAINT address_pkey PRIMARY KEY (address_id);

ALTER TABLE customer
    ADD CONSTRAINT customer_pkey PRIMARY KEY (cust_id),
    ADD CONSTRAINT customer_aid_fkey FOREIGN KEY (address_id) REFERENCES address(address_id),
    ADD CONSTRAINT customer_pid_fkey FOREIGN KEY (plan_id) REFERENCES rentalplan(plan_id);

/* TODO: movie_id constraint rework */
ALTER TABLE activerental
    ADD CONSTRAINT activerental_mid_key UNIQUE (movie_id), -- restriction 1
    ADD CONSTRAINT activerental_pkey PRIMARY KEY (rental_id),
    -- ADD CONSTRAINT activerental_mid_fkey FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    ADD CONSTRAINT activerental_cid_fkey FOREIGN KEY (cust_id) REFERENCES customer(cust_id);

ALTER TABLE inactiverental
    ADD CONSTRAINT rentalhistory_pkey PRIMARY KEY (rental_id),
    -- ADD CONSTRAINT rentalhistory_mid_fkey FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    ADD CONSTRAINT rentalhistory_cid_fkey FOREIGN KEY (cust_id) REFERENCES customer(cust_id);

/** Preserve records that are deleted from activerental in historical inactiverental
* and save date of return
*/
CREATE OR REPLACE FUNCTION copy_to_history()
RETURNS trigger AS
$end_rental$
    BEGIN
        INSERT INTO inactiverental (rental_id, movie_id, cust_id, datein, dateout)
        VALUES (OLD.rental_id, OLD.movie_id, OLD.cust_id, OLD.dateout, current_timestamp);
        RETURN NULL;
    END;
$end_rental$
LANGUAGE plpgsql;

/** When any row is deleted from activerental, call copy_to_history()
*/
DROP TRIGGER IF EXISTS end_rental ON activerental;
CREATE TRIGGER end_rental
AFTER DELETE
ON activerental
FOR EACH ROW
EXECUTE PROCEDURE copy_to_history();

/** Rollback insertion into activerental if the number of active rentals for a customer
* would exceed their allowed plan amount
*/
CREATE OR REPLACE FUNCTION check_outstanding()
RETURNS trigger AS
$check_overdue$
    DECLARE custid customer.cust_id%TYPE;
        planid customer.plan_id%TYPE;
        outstanding integer;
        allowedrentals integer;
    BEGIN
        custid := NEW.cust_id;

        SELECT count(*) INTO outstanding
        FROM activerental
        WHERE cust_id = custid;

        SELECT r.maxrentals INTO allowedrentals
        FROM rentalplan r
        INNER JOIN customer c on c.plan_id = r.plan_id
        WHERE c.cust_id = custid;
        RAISE NOTICE 'For: % Outstanding: % Allowed: %',custid, outstanding, allowedrentals;

        IF outstanding>allowedrentals THEN -- restriction 2b
            ROLLBACK TRANSACTION;
        END IF;
        RETURN NULL;
    END;
$check_overdue$
LANGUAGE plpgsql;

/** When any row is inserted into activerental, call check_outstanding()
*/
DROP TRIGGER IF EXISTS check_overdue ON activerental;
CREATE TRIGGER check_overdue
AFTER INSERT
ON activerental
FOR EACH ROW
EXECUTE PROCEDURE check_outstanding();

/* Initial Data */
INSERT INTO rentalplan (name, maxrentals, fee)
VALUES    ('none', 0, 0),
    ('basic', 1, 1.99),
    ('rental plus', 3, 2.99),
    ('super access', 5, 3.99),
    ('prime', 10, 4.99);

INSERT INTO address (street, city, state, zip)
VALUES    ('123 Campus Center', 'Amherst', '01003', 'Massachusetts'),
    ('456 CS Building', 'Amherst', '01003', 'Massachusetts'),
    ('789 Student Union', 'Amherst', '01003', 'Massachusetts'),
    ('012 Goessman', 'Amherst', '01003', 'Massachusetts'),
    ('345 Parking Garage', 'Amherst', '01003', 'Massachusetts'),
    ('678 Mullins Center', 'Amherst', '01003', 'Massachusetts');

INSERT INTO customer (username, password, fname, lname, phone, plan_id, address_id)
VALUES     ('aelsey', 'abc', 'Andrew', 'Elsey', '4131111111', 1, 1),
     ('asantos', 'def', 'Anthony', 'Santos', '4132222222', 2, 2),
    ('danton', 'ghi', 'Derek', 'Anton', '4133333333', 3, 3),
    ('jfrankline', 'jkl', 'Jonathan', 'Frankline', '4134444444', 4, 4),
    ('pghale', 'mno', 'Pratima', 'Ghale', '4135555555', 1, 5),
    ('tpham', 'pqr', 'Ted', 'Pham', '4136666666', 2, 6);

INSERT INTO activerental (movie_id, cust_id, dateout)
VALUES    (1, 2, current_timestamp - interval '2 day'),
    (3, 4, current_timestamp - interval '4 day');

DELETE FROM activerental
WHERE cust_id = 4;

--ensure transactions are synchronized
SET default_transaction_isolation TO serializable;
