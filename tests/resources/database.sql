CREATE DATABASE testing;
USE testing;

CREATE TABLE inner2 (
	id    INT NOT NULL PRIMARY KEY,
	value DATE
);
INSERT INTO inner2 VALUES (-1, '1970-01-01 00:00:00');
INSERT INTO inner2 VALUES (-2, NULL);

CREATE TABLE inner1 (
	id    INT NOT NULL PRIMARY KEY,
	value VARCHAR(20),
	time  INT,
	FOREIGN KEY (time) REFERENCES inner2 (id)
);
INSERT INTO inner1 VALUES (1, 'a', -1);
INSERT INTO inner1 VALUES (2, 'b', -2);
INSERT INTO inner1 VALUES (3, 'c', NULL);
INSERT INTO inner1 VALUES (4, 'd', NULL);

CREATE TABLE fact_table (
	id    INT NOT NULL PRIMARY KEY,
	name VARCHAR(20),
	about INT,
	FOREIGN KEY (about) REFERENCES inner1 (id)
);
INSERT INTO fact_table VALUES (10, 'A', 1);
INSERT INTO fact_table VALUES (11, 'B', 2);
INSERT INTO fact_table VALUES (12, 'C', 3);
INSERT INTO fact_table VALUES (13, 'D', NULL);
INSERT INTO fact_table VALUES (15, 'E', 1);
INSERT INTO fact_table VALUES (16, 'F', 2);
INSERT INTO fact_table VALUES (17, 'G', 3);
INSERT INTO fact_table VALUES (18, 'H', NULL);
INSERT INTO fact_table VALUES (19, 'I', 1);
INSERT INTO fact_table VALUES (20, 'J', 2);
INSERT INTO fact_table VALUES (21, 'K', 3);
INSERT INTO fact_table VALUES (22, 'L', NULL);
INSERT INTO fact_table VALUES (30, NULL, NULL);
INSERT INTO fact_table VALUES (40, 'N', 4);


CREATE TABLE nested1 (
	id          INT NOT NULL PRIMARY KEY,
	ref         INT,
	description VARCHAR(20),
	about       INT,
	FOREIGN KEY (ref) REFERENCES fact_table (id),
	FOREIGN KEY (about) REFERENCES inner2 (id)
);
insert into nested1 VALUES (100, 10, 'aaa', -1);
insert into nested1 VALUES (101, 11, 'bbb', -2);
insert into nested1 VALUES (102, 12, 'ccc', NULL);
insert into nested1 VALUES (103, 13, 'ddd', -1);

insert into nested1 VALUES (104, 15, 'eee', -1);
insert into nested1 VALUES (105, 15, 'fff', -1);
insert into nested1 VALUES (106, 16, 'ggg', -2);
insert into nested1 VALUES (107, 16, 'hhh', NULL);
insert into nested1 VALUES (108, 17, 'iii', -2);
insert into nested1 VALUES (109, 17, 'jjj', -2);
insert into nested1 VALUES (110, 18, 'kkk', NULL);
insert into nested1 VALUES (111, 18, 'lll', NULL);
insert into nested1 VALUES (130, 30, NULL, NULL);


CREATE TABLE nested2 (
	id      INT NOT NULL PRIMARY KEY,
	ref     INT,
	minutia DOUBLE PRECISION,
	about   INT,
	FOREIGN KEY (ref) REFERENCES nested1 (id),
	FOREIGN KEY (about) REFERENCES inner1 (id)
);
insert into nested2 VALUES (1000, 100, 3.1415926539, 1);
insert into nested2 VALUES (1001, 100, 4.0, 2);
insert into nested2 VALUES (1002, 100, 5.1, 3);
insert into nested2 VALUES (1003, 101, 6.2, 1);
insert into nested2 VALUES (1004, 102, 7.3, 3);
insert into nested2 VALUES (1030, 130, 8.4, 3);


CREATE TABLE z_facts (
    id    INT NOT NULL PRIMARY KEY,
    name VARCHAR(20)
);
INSERT INTO z_facts VALUES (8, 'A');
INSERT INTO z_facts VALUES (9, 'B');

CREATE TABLE z_inner (
    id          INT NOT NULL PRIMARY KEY,
    more        INT,
    FOREIGN KEY (more) REFERENCES z_facts (id)
);
insert into z_inner VALUES (18, 9);
insert into z_inner VALUES (19, 9);

CREATE TABLE z_nested
(
    id   INT NOT NULL PRIMARY KEY,
    ref  INT,
    details INT,
    FOREIGN KEY (ref) REFERENCES z_facts (id),
    FOREIGN KEY (details) REFERENCES z_inner (id)
);
INSERT INTO z_nested VALUES (108, 8, 18);
INSERT INTO z_nested VALUES (109, 8, 19);










commit;
