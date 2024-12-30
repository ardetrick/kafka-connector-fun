CREATE TABLE my_table (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(50),
    value VARCHAR2(50)
);

INSERT INTO my_table (id, name, value) VALUES (1, 'Test Name', 'Test Value');

INSERT INTO my_table (id, name, value) VALUES (2, 'Another Name', 'Another Value');

commit;