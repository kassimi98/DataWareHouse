
------------------------------------------------------------------------------------------------------------------------------------------------
------||||||||||||||||||||||||||||||||||||||||||||||||||| Phase d'Extraction |||||||||||||||||||||||||||||||||||||||||||||||||||||||---
----------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE DIRECTORY dataWareHouseSrc AS 'lien';---F:\DATASET\';


----------------------------------------------------| Products Table |-----------------------------------------------------------------------
drop table Products;

CREATE TABLE Products (
product_id INTEGER,
product_name Varchar2(50),
aisle_id INTEGER,
department_id INTEGER
)

ORGANIZATION EXTERNAL(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dataWareHouseSrc
    ACCESS PARAMETERS(
        RECORDS DELIMITED BY newline
        Skip 1
        FIELDS TERMINATED BY ','
    )
LOCATION('products.csv'))
REJECT LIMIT UNLIMITED ;

select count(*) from Products;

-------------------------------------------------------| Commandes Table |---------------------------------------------------------------------------------------

drop table Commandes;

CREATE TABLE Commandes (

order_id INTEGER,
user_id INTEGER,
eval_set varchar2(10),
order_number INTEGER,
order_dow INTEGER,
order_hour_of_day INTEGER,
days_since_prior_order INTEGER

)

ORGANIZATION EXTERNAL(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dataWareHouseSrc
    ACCESS PARAMETERS(
        RECORDS DELIMITED BY newline
        Skip 1
        FIELDS TERMINATED BY ','
    )
LOCATION('Commandes.csv'))
REJECT LIMIT UNLIMITED ;

select count(*) from Commandes where eval_set='prior' 
and order_id in (select order_id from order_products__prior where 
product_id in (select product_id from Products))


--3214874
----------------------------------------------------|   aisles Table |----------------------------------------------------------------------------------

drop table aisles;

CREATE TABLE aisles (

aisle_id integer,
aisle VARCHAR(50)

)

ORGANIZATION EXTERNAL(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dataWareHouseSrc
    ACCESS PARAMETERS(
        RECORDS DELIMITED BY newline
        Skip 1
        FIELDS TERMINATED BY ','
    )
LOCATION('aisles.csv'))
REJECT LIMIT UNLIMITED ;

select count(*) from aisles;
-- 134 ligne


------------------------------------------------------| departments Table |---------------------------------------------------------------------------


drop table departments;

CREATE TABLE departments (

department_id integer,
department VARCHAR(50)


)

ORGANIZATION EXTERNAL(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dataWareHouseSrc
    ACCESS PARAMETERS(
        RECORDS DELIMITED BY newline
        Skip 1
        FIELDS TERMINATED BY ','
    )
LOCATION('departments.csv'))
REJECT LIMIT UNLIMITED ;

select count(*) from departments;
--- 21 ligne

----------------------------------------------------| order_products__prior_ Table |-------------------------------------------------------------------

drop table order_products__prior;

CREATE TABLE order_products__prior (


order_id  INTEGER,
product_id INTEGER,
add_to_cart_order INTEGER,
reordered INTEGER

)

ORGANIZATION EXTERNAL(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dataWareHouseSrc
    ACCESS PARAMETERS(
        RECORDS DELIMITED BY newline
        Skip 1
        FIELDS TERMINATED BY ','
    )
LOCATION('order_products__prior_.csv'))
REJECT LIMIT UNLIMITED ;

select count(*) from order_products__prior;
--- 20501 ligne

-----------------------------------------------------------------------------------------------------------------------------------------------------------
------||||||||||||||||||||||||||||||||||||||||||||||||||| Phase de Transformation |||||||||||||||||||||||||||||||||||||||||||||||||||||||---
-----------------------------------------------------------------------------------------------------------------------------------------------------------


-------------------------------------------------------| BDT_Products TABLE |--------------------------------------------------------------


CREATE OR REPLACE TYPE T_Products AS OBJECT (
    product_id INTEGER,
    product_name Varchar2(50),
    aisle_id INTEGER,
    department_id INTEGER,
    MEMBER FUNCTION fun_product_id RETURN INTEGER,
    MEMBER FUNCTION fun_product_name RETURN VARCHAR2,
    MEMBER FUNCTION fun_aisle_id RETURN INTEGER,
    MEMBER FUNCTION fun_department_id RETURN INTEGER
   
);
/

CREATE OR REPLACE TYPE BODY T_Products AS
    MEMBER FUNCTION fun_product_id RETURN INTEGER is
    
    BEGIN
        RETURN product_id;
    END;
    
   MEMBER FUNCTION fun_product_name RETURN VARCHAR2 IS
    BEGIN
        RETURN product_name;
    END;
    
    MEMBER FUNCTION fun_aisle_id RETURN INTEGER IS
    BEGIN
        RETURN aisle_id;
    END;
    
    MEMBER FUNCTION fun_department_id RETURN INTEGER IS
    BEGIN
        RETURN department_id;
    END;
    
END;
/

DROP TABLE BDT_Products;
CREATE TABLE BDT_Products OF T_Products;
---------------------------------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE BDT_Products ADD CONSTRAINT pk_product_id  PRIMARY KEY (product_id);

ALTER TABLE BDT_Products ADD CONSTRAINT fk_aisle_id 
FOREIGN KEY (aisle_id) REFERENCES BDT_aisles(aisle_id);

ALTER TABLE BDT_Products ADD CONSTRAINT fk_department_id 
FOREIGN KEY (department_id) REFERENCES BDT_departments(department_id);


ALTER TABLE BDT_Products disable CONSTRAINT fk_aisle_id;
ALTER TABLE BDT_Products disable CONSTRAINT fk_department_id;


ALTER TABLE BDT_Products ENABLE CONSTRAINT fk_aisle_id;
ALTER TABLE BDT_Products ENABLE CONSTRAINT fk_department_id;

ALTER TABLE BDT_Products DISABLE CONSTRAINT pk_product_id ;
----------------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO BDT_Products SELECT * FROM Products 
where product_id in 
(select product_id from  order_products__prior
where order_id in (select order_id from  Commandes where eval_set='prior'));

ALTER TABLE BDT_Products ENABLE CONSTRAINT pk_product_id;

select count(*) from BDT_Products;


------------------------------------------------------| BDT_Commandes TABLE |--------------------------------------------------------------


select * from Commandes;

CREATE or replace  type T_Commandes as object (

order_id INTEGER,
user_id INTEGER,
eval_set varchar2(10),
order_number INTEGER,
order_dow INTEGER,
order_hour_of_day INTEGER,
days_since_prior_order INTEGER,

MEMBER FUNCTION fun_order_id RETURN INTEGER,
MEMBER FUNCTION fun_user_id RETURN INTEGER,
MEMBER FUNCTION fun_eval_set RETURN varchar2,
MEMBER FUNCTION fun_order_number RETURN INTEGER,
MEMBER FUNCTION fun_order_dow RETURN INTEGER,
MEMBER FUNCTION fun_order_hour_of_day RETURN INTEGER,
MEMBER FUNCTION fun_days_since_prior_order RETURN INTEGER

);
/
CREATE OR REPLACE TYPE BODY T_Commandes as 


MEMBER FUNCTION fun_order_id RETURN integer is
BEGIN 
        return order_id;
END;

MEMBER FUNCTION fun_user_id RETURN integer is
BEGIN 
        return user_id;
END;

MEMBER FUNCTION fun_eval_set RETURN varchar2 is 

BEGIN 
        return eval_set;
END;

MEMBER FUNCTION fun_order_number RETURN integer is
BEGIN 
        return order_number;
END;

MEMBER FUNCTION fun_order_dow RETURN integer is
BEGIN 
        return order_dow;
END;

MEMBER FUNCTION fun_order_hour_of_day RETURN integer is
BEGIN 
        return order_hour_of_day;
END;

MEMBER FUNCTION fun_days_since_prior_order RETURN integer is
BEGIN 
    if days_since_prior_order is null then
        return 0;
    else
        return days_since_prior_order;
    end if;
END;

END;
/

DROP TABLE BDT_Commandes;
CREATE TABLE BDT_Commandes OF T_Commandes;
---------------------------------------------------------------------------------------------------------------------------------------------------------

ALTER TABLE BDT_Commandes ADD CONSTRAINT pk_order_id  PRIMARY KEY (order_id);

ALTER TABLE BDT_Commandes DISABLE CONSTRAINT pk_order_id;
----------------------------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE BDT_Commandes ENABLE CONSTRAINT pk_order_id;


INSERT INTO BDT_Commandes SELECT * FROM  Commandes where eval_set='prior' 
and order_id in (select order_id from order_products__prior where 
product_id in (select product_id from Products))

select * from BDT_Commandes b ;


----------------------------------------------------------| BDT_aisles TABLE |-------------------------------------------------------------


CREATE or replace  type T_aisles as object (

aisle_id integer,
aisle VARCHAR2(50),
MEMBER FUNCTION fun_aisle_id return INTEGER,
MEMBER FUNCTION fun_aisle return varchar2

);
/
CREATE OR REPLACE TYPE BODY T_aisles As
MEMBER FUNCTION fun_aisle_id return INTEGER is
    BEGIN
        RETURN aisle_id;
    END;
MEMBER FUNCTION fun_aisle return VARCHAR2 is
    BEGIN
        RETURN aisle;
    END;
END;
/

DROP TABLE BDT_aisles;
CREATE TABLE BDT_aisles OF T_aisles;

---------------------------------------------------------------------------------------------------------------------------------------------------------

ALTER TABLE BDT_aisles ADD CONSTRAINT pk_aisle_id  PRIMARY KEY (aisle_id);

ALTER TABLE BDT_aisles DISABLE CONSTRAINT pk_aisle_id;
----------------------------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE BDT_aisles ENABLE CONSTRAINT pk_aisle_id;
INSERT INTO BDT_aisles SELECT * FROM aisles;
select   b.fun_aisle_id() from BDT_aisles b;


----------------------------------------------------| BDT_departement TABLE |-------------------------------------------------------------


CREATE or replace  type T_departments as object (

department_id integer,
department VARCHAR2(50),
MEMBER FUNCTION fun_department_id return INTEGER,
MEMBER FUNCTION fun_department return varchar2

);
/
CREATE OR REPLACE TYPE BODY T_departments As
MEMBER FUNCTION fun_department_id return INTEGER is
    BEGIN
        RETURN department_id;
    END;
MEMBER FUNCTION fun_department return VARCHAR2 is
    BEGIN
        RETURN department;
    END;
END;
/

DROP TABLE BDT_departments;
CREATE TABLE BDT_departments OF T_departments;

---------------------------------------------------------------------------------------------------------------------------------------------------------

ALTER TABLE BDT_departments ADD CONSTRAINT pk_department_id  PRIMARY KEY (department_id);

ALTER TABLE BDT_departments disable CONSTRAINT pk_department_id;
----------------------------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE BDT_departments ENABLE CONSTRAINT pk_department_id;

INSERT INTO BDT_departments SELECT * FROM departments ;
select   b.fun_department_id() from BDT_departments b;


---------------------------------------------------|   BDT_order_products__prior TABLE    |--------------------------------------------

CREATE or replace  type T_order_products__prior as object (

order_id  INTEGER,
product_id INTEGER,
add_to_cart_order INTEGER,
reordered INTEGER,
MEMBER FUNCTION fun_order_id return INTEGER,
MEMBER FUNCTION fun_product_id return INTEGER,
MEMBER FUNCTION fun_add_to_cart_order return INTEGER,
MEMBER FUNCTION fun_reordered return INTEGER

);
/
CREATE OR REPLACE TYPE BODY T_order_products__prior As

MEMBER FUNCTION fun_order_id return INTEGER is
    BEGIN
        RETURN order_id;
    END;
MEMBER FUNCTION fun_product_id return INTEGER is
    BEGIN
        RETURN product_id;
    END;
MEMBER FUNCTION fun_add_to_cart_order return INTEGER is
    BEGIN
        RETURN add_to_cart_order;
    END;
MEMBER FUNCTION fun_reordered return INTEGER is
    BEGIN
        RETURN reordered;
    END;
END;
/

DROP TABLE  BDT_order_products__prior;
CREATE TABLE BDT_order_products__prior OF T_order_products__prior;

-----------------------------------------------------------------constraints ---------------------------------------------------------------------------
ALTER TABLE BDT_order_products__prior ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES BDT_Commandes(order_id);
ALTER TABLE BDT_order_products__prior ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES BDT_Products(product_id);
ALTER TABLE BDT_order_products__prior ADD CONSTRAINT pk_order_product_id  PRIMARY KEY (order_id,product_id);



ALTER TABLE BDT_order_products__prior disable CONSTRAINT fk_order_id;
ALTER TABLE BDT_order_products__prior disable CONSTRAINT fk_product_id;
--ALTER TABLE BDT_order_products__prior DISABLE CONSTRAINT pk_order_product_id;


---------------------------------------------------------------------------------------------------------------------------------------------------------

INSERT INTO BDT_order_products__prior SELECT * FROM order_products__prior 
where order_id in (select order_id from Commandes where eval_set='prior')
and product_id in (select product_id from Products) ;


ALTER TABLE BDT_order_products__prior ENABLE CONSTRAINT fk_order_id;
ALTER TABLE BDT_order_products__prior ENABLE CONSTRAINT fk_product_id;
ALTER TABLE BDT_order_products__prior ENABLE CONSTRAINT pk_order_product_id;


select * from BDT_order_products__prior



--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------||||||||||||||||| Phase DataWareHouse |||||||||||||||||||||||-----------------------------------
--------------------------------------------------------------------------------------------------------------------------------------



drop table VENTES_FACT
CREATE table VENTES_FACT (

order_id_DW  INTEGER,
product_id_DW INTEGER

); 

alter table VENTES_FACT add constraint fk_order_id_DW 
FOREIGN KEY (order_id_DW) REFERENCES COMMANDES_DIM(order_id_DW);

alter table VENTES_FACT add constraint fk_product_id_DW 
FOREIGN KEY (product_id_DW) REFERENCES PRODUITS_DIM(product_id_DW);

alter table VENTES_FACT add constraint pk_order_id_product_id_DW primary key(order_id_DW,product_id_DW);



alter table VENTES_FACT disable constraint pk_order_id_product_id_DW;
alter table VENTES_FACT disable constraint fk_order_id_DW;
alter table VENTES_FACT disable constraint fk_product_id_DW;

INSERT INTO VENTES_FACT 
SELECT opp.fun_order_id(), opp.fun_product_id() FROM BDT_order_products__prior opp;

alter table VENTES_FACT enable constraint pk_order_id_product_id_DW;
alter table VENTES_FACT enable constraint fk_order_id_DW;
alter table VENTES_FACT enable constraint fk_product_id_DW;

select * from VENTES_FACT

------------------------------------------------------------| TABLE PRODUITS_DIM |-----------------------------------------------------


drop table PRODUITS_DIM
CREATE table PRODUITS_DIM (

product_id_DW INTEGER,
product_name_DW varchar2(50),
aisle_id_DW INTEGER,
department_id_DW INTEGER

); 
alter table PRODUITS_DIM add constraint pk_product_id_DW primary key(product_id_DW);

alter table PRODUITS_DIM add constraint fk_aisle_id_DW foreign key(aisle_id_DW) 
REFERENCES AISLES_DIM(aisle_id_DW);

alter table PRODUITS_DIM add constraint fk_department_id_DW foreign key(department_id_DW) 
REFERENCES DEPARTEMENT_DIM(department_id_DW);

alter table PRODUITS_DIM disable constraint pk_product_id_DW;
alter table PRODUITS_DIM disable constraint fk_aisle_id_DW;
alter table PRODUITS_DIM disable constraint fk_department_id_DW;


INSERT INTO PRODUITS_DIM 
SELECT p.fun_product_id(), p.fun_product_name(),p.fun_aisle_id(),p.fun_department_id()
FROM BDT_Products p;


alter table PRODUITS_DIM enable constraint pk_product_id_DW;
alter table PRODUITS_DIM enable constraint fk_aisle_id_DW;
alter table PRODUITS_DIM enable constraint fk_department_id_DW;


--------------------------------------------------------|   COMMANDES_DIM   |--------------------------------------------------------------


CREATE table COMMANDES_DIM (

order_id_DW INTEGER,
user_id_DW INTEGER,
order_number_DW INTEGER,
order_dow_DW INTEGER,
order_hour_of_day_DW INTEGER,
days_since_prior_order_DW INTEGER

);

alter table COMMANDES_DIM add constraint pk_order_id_DW primary key(order_id_DW);

alter table COMMANDES_DIM disable constraint pk_order_id_DW;

INSERT INTO COMMANDES_DIM
SELECT c.fun_order_id(), c.fun_user_id(),
c.fun_order_number() ,c.fun_order_dow(),c.fun_order_hour_of_day(),c.fun_days_since_prior_order()

FROM BDT_Commandes c;

alter table COMMANDES_DIM enable constraint pk_order_id_DW;

select * from COMMANDES_DIM


--------------------------------------------------|     DEPARTEMENT_DIM       |------------------------------------------------------------


CREATE table DEPARTEMENT_DIM (

department_id_DW integer,
department_DW VARCHAR2(50)

);

alter table DEPARTEMENT_DIM add constraint pk_department_id_DW primary key (department_id_DW);
alter table DEPARTEMENT_DIM disable constraint pk_department_id_DW;

INSERT INTO DEPARTEMENT_DIM 
SELECT d.fun_department_id(), d.fun_department() FROM BDT_departments d;

alter table DEPARTEMENT_DIM enable constraint pk_department_id_DW


--------------------------------------------------------|   AISLES_DIM  |---------------------------------------------------------------


CREATE table AISLES_DIM (

aisle_id_DW integer,
aisle VARCHAR2(50)

);

alter table AISLES_DIM add constraint pk_aisle_id_DW primary key (aisle_id_DW);
alter table AISLES_DIM disable constraint pk_aisle_id_DW;

INSERT INTO AISLES_DIM
SELECT a.fun_aisle_id(), a.fun_aisle() FROM BDT_aisles a;

alter table AISLES_DIM enable constraint pk_aisle_id_DW;

--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------||||||||||||||||| Phase D'EXploration |||||||||||||||||||||||-----------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
-- Requete 1: -- Les 10 allées de magasin qui contiennent les produits les plus vendus ;


select aisle from AISLES_DIM 
where aisle_id_DW IN ( select aisle_id_DW from PRODUITS_DIM where product_id_DW IN (
    SELECT  *
    FROM    (
            SELECT  PRODUCT_ID_DW
            from    VENTES_FACT 
            group by PRODUCT_ID_DW
            order by count(*) desc
            )
    WHERE   rownum <= 10
));


-- Requete 2 : Les temps du jour dans lequel les clients achètent un produit.
-- Example produit : 'Petit Suisse Fruit' ;

select order_hour_of_day_DW from COMMANDES_DIM where order_id_DW IN (
    select order_id_DW from 
    VENTES_FACT where product_id_DW IN (select product_id_DW from PRODUITS_DIM where product_name_DW LIKE '%Petit Suisse Fruit%')
    )
    GROUP BY order_hour_of_day_DW
HAVING count(order_hour_of_day_DW) IN ( select  MAX(count(order_hour_of_day_DW)) from COMMANDES_DIM where order_id_DW IN (
    select order_id_DW from 
    VENTES_FACT where product_id_DW IN (select product_id_DW from PRODUITS_DIM where product_name_DW LIKE '%Petit Suisse Fruit%')
    )
    GROUP BY order_hour_of_day_DW )
;


-- Requete 3:  Les dix produits les plus vendus en Vendredi de 8H à 12H;

SELECT product_name_DW, product_id_DW FROM PRODUITS_DIM JOIN ( SELECT product_id_DW, count(product_id_DW) FROM VENTES_FACT WHERE order_id_DW IN(
    SELECT order_id_DW from COMMANDES_DIM where (order_hour_of_day_DW BETWEEN 8 AND 12) AND (order_dow_DW = 5)
)
GROUP BY product_id_DW
ORDER BY count(product_id_DW) DESC) USING (product_id_DW) WHERE rownum <= 10;


----------------------------------------------------------------------------------------------------------
----------------------------||||||||||||||||| Phase D'optimisation ||||||||||||||||||||||||||||||---------
----------------------------------------------------------------------------------------------------------

---------------------------------------------- Requete 1 ------------------------------------------------

EXPLAIN PLAN FOR select aisle from AISLES_DIM 
where aisle_id_DW IN ( select aisle_id_DW from PRODUITS_DIM where product_id_DW IN (
    SELECT  *
    FROM    (
            SELECT  PRODUCT_ID_DW
            from    VENTES_FACT 
            group by PRODUCT_ID_DW
            order by count(*) desc
            )
    WHERE   rownum <= 10
));
select * from table(dbms_xplan.display);

CREATE INDEX index_ventes ON VENTES_FACT(order_id_DW);

DROP INDEX index_ventes;


---------------------------------------------- Requete 2 ------------------------------------------------

EXPLAIN PLAN FOR select order_hour_of_day_DW from COMMANDES_DIM where order_id_DW IN (
select order_id_DW from 
VENTES_FACT where product_id_DW IN (select product_id_DW from PRODUITS_DIM where product_name_DW LIKE '%All-Seasons Salt%')
)
GROUP BY order_hour_of_day_DW
HAVING count(order_hour_of_day_DW) IN ( select  MAX(count(order_hour_of_day_DW)) from COMMANDES_DIM where order_id_DW IN (
select order_id_DW from 
VENTES_FACT where product_id_DW IN (select product_id_DW from PRODUITS_DIM where product_name_DW LIKE '%All-Seasons Salt%')
)
GROUP BY order_hour_of_day_DW )
;
select * from table(dbms_xplan.display);

CREATE INDEX index_commandes ON COMMANDES_DIM(order_id_DW);


---------------------------------------------- Requete 3 ------------------------------------------------

EXPLAIN PLAN FOR SELECT product_name_DW FROM PRODUITS_DIM JOIN ( SELECT product_id_DW, count(product_id_DW) FROM VENTES_FACT WHERE order_id_DW IN(
    SELECT order_id_DW from COMMANDES_DIM where (order_hour_of_day_DW BETWEEN 8 AND 12) AND (order_dow_DW = 5)
)
GROUP BY product_id_DW
ORDER BY count(product_id_DW) DESC) USING (product_id_DW) WHERE rownum <= 10;

select * from table(dbms_xplan.display);


