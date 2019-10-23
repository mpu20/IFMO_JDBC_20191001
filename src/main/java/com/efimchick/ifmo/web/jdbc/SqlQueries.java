package com.efimchick.ifmo.web.jdbc;
/**
 * Implement sql queries like described
 */
public class SqlQueries {
    //Select all employees sorted by last name in ascending order
    //language=HSQLDB
    String select01 = "SELECT ID, LASTNAME, SALARY from EMPLOYEE ORDER BY LASTNAME ASC";

    //Select employees having no more than 5 characters in last name sorted by last name in ascending order
    //language=HSQLDB
    String select02 = "SELECT ID, LASTNAME, SALARY FROM EMPLOYEE WHERE LENGTH(LASTNAME) < 6 ORDER BY LASTNAME ASC";

    //Select employees having salary no less than 2000 and no more than 3000
    //language=HSQLDB
    String select03 = "SELECT ID, LASTNAME, SALARY FROM EMPLOYEE WHERE SALARY >= 2000.00 AND SALARY <= 3000.00";

    //Select employees having salary no more than 2000 or no less than 3000
    //language=HSQLDB
    String select04 = "SELECT ID, LASTNAME, SALARY FROM EMPLOYEE WHERE SALARY <= 2000.00 OR SALARY >= 3000.00";

    //Select employees assigned to a department and corresponding department name
    //language=HSQLDB
    String select05 = "SELECT EMPLOYEE.LASTNAME, EMPLOYEE.SALARY, DEPARTMENT.NAME FROM EMPLOYEE INNER JOIN DEPARTMENT ON EMPLOYEE.DEPARTMENT = DEPARTMENT.ID";

    //Select all employees and corresponding department name if there is one.
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select06 = "SELECT EMPLOYEE.LASTNAME, EMPLOYEE.SALARY, DEPARTMENT.NAME AS depname FROM EMPLOYEE LEFT OUTER JOIN DEPARTMENT ON EMPLOYEE.DEPARTMENT = DEPARTMENT.ID";

    //Select total salary pf all employees. Name it "total".
    //language=HSQLDB
    String select07 = "SELECT SUM(SALARY) AS total FROM EMPLOYEE";

    //Select all departments and amount of employees assigned per department
    //Name column containing name of the department "depname".
    //Name column containing employee amount "staff_size".
    //language=HSQLDB
    String select08 = "SELECT DEPARTMENT.NAME AS depname, COUNT(DEPARTMENT.NAME) AS staff_size FROM EMPLOYEE INNER JOIN DEPARTMENT on DEPARTMENT.ID = EMPLOYEE.DEPARTMENT GROUP BY DEPARTMENT.NAME";

    //Select all departments and values of total and average salary per department
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select09 = "SELECT DEPARTMENT.NAME AS depname, SUM(EMPLOYEE.SALARY) AS total, AVG(EMPLOYEE.SALARY) AS average FROM EMPLOYEE INNER JOIN DEPARTMENT on DEPARTMENT.ID = EMPLOYEE.DEPARTMENT GROUP BY DEPARTMENT.NAME";

    //Select all employees and their managers if there is one.
    //Name column containing employee lastname "employee".
    //Name column containing manager lastname "manager".
    //language=HSQLDB
    String select10 = "SELECT em.LASTNAME AS employee, ma.LASTNAME AS manager FROM EMPLOYEE AS em LEFT OUTER JOIN EMPLOYEE AS ma ON em.MANAGER = ma.ID";


}
