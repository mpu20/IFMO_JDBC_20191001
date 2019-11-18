package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class ServiceFactory {

    private Statement getStatement() throws SQLException {
        Connection connection = ConnectionSource.instance().createConnection();
        return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public EmployeeService employeeService(){
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY HIREDATE", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY LASTNAME", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY SALARY", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY DEPARTMENT, LASTNAME", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() +" ORDER BY HIREDATE", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() +" ORDER BY SALARY", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() +" ORDER BY LASTNAME", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId().toString() +" ORDER BY LASTNAME", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId().toString() +" ORDER BY HIREDATE", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId().toString() +" ORDER BY SALARY", getAllEmployee(false, getAllDepartment()));
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE ID = " + employee.getId().toString(), getAllEmployee(true, getAllDepartment()));
                    return list.get(0);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                try {
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() + " ORDER BY SALARY DESC", getAllEmployee(false, getAllDepartment()));
                    return list.get(salaryRank-1);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }

    private List<Employee> getEmployees(Paging paging, List<Employee> list) {
        List<Employee> ans = new ArrayList<>();
        for (int i = paging.itemPerPage*(paging.page-1); i < paging.itemPerPage*paging.page && i < list.size(); i++) {
            ans.add(list.get(i));
        }
        return ans;
    }

    private List<Employee> getEmployeesOrder(String sql, List<Employee> le) throws SQLException {
        Statement ps = getStatement();
        ResultSet resultSet = ps.executeQuery(sql);
        List<Employee> list_res = new ArrayList<>();
        while (resultSet.next()) {
            int id = resultSet.getInt("ID");
            for (Employee e : le) {
                if (id == e.getId().intValue()) {
                    list_res.add(e);
                }
            }
        }
        return list_res;
    }

    private Employee getInfoEmployee(BigInteger Mid, ResultSet resultSet, int numberChain, boolean Chain, List<Department> ld) {
        try {
            while (resultSet.next()) {
                BigInteger id = new BigInteger(resultSet.getString("ID"));
                if (Mid.equals(id)) {
                    String first_name = resultSet.getString("FIRSTNAME");
                    String middle_name = resultSet.getString("MIDDLENAME");
                    String last_name = resultSet.getString("LASTNAME");
                    FullName full_name = new FullName(first_name, last_name, middle_name);
                    Position pos = Position.valueOf(resultSet.getString("POSITION"));
                    LocalDate date_hire = LocalDate.parse(resultSet.getString("HIREDATE"));
                    BigDecimal salary = resultSet.getBigDecimal("SALARY");
                    int managerid = resultSet.getInt("MANAGER");
                    Employee manager = null;
                    if (managerid != 0 && (Chain || numberChain<1)) {
                        int current = resultSet.getRow();
                        resultSet.beforeFirst();
                        manager = getInfoEmployee(BigInteger.valueOf(managerid), resultSet, numberChain+1, Chain, ld);
                        resultSet.absolute(current);
                    }
                    Department department = null;
                    for (Department department1 : ld) {
                        if (department1.getId().equals(BigInteger.valueOf(resultSet.getInt("DEPARTMENT"))))
                            department = department1;
                    }
                    return new Employee(id, full_name, pos, date_hire, salary, manager, department);
                }
            }
            return null;
        } catch (SQLException e) {
            return null;
        }
    }

    private List<Employee> getAllEmployee(boolean Chain, List<Department> ld) throws SQLException {
        Statement ps = getStatement();
        ResultSet resultSet = ps.executeQuery("SELECT * FROM EMPLOYEE");
        List<Employee> le = new ArrayList<>();
        while (resultSet.next()) {
            int current = resultSet.getRow();
            BigInteger id = new BigInteger(resultSet.getString("ID"));
            resultSet.beforeFirst();
            le.add(getInfoEmployee(id, resultSet, 0, Chain, ld));
            resultSet.absolute(current);
        }
        return le;
    }

    private List<Department> getAllDepartment() throws SQLException {
        Statement ps = getStatement();
        ResultSet resultSet = ps.executeQuery("SELECT * FROM DEPARTMENT");
        List<Department> ld = new ArrayList<>();
        while (resultSet.next()) {
            ld.add(new Department(BigInteger.valueOf(resultSet.getInt("ID")), resultSet.getString("NAME"), resultSet.getString("LOCATION")));
        }
        return ld;
    }
}

