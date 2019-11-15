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
    private List<Department> ld = new ArrayList<>();
    private List<Employee> le = new ArrayList<>();

    private Statement getStatement() throws SQLException {
        Connection connection = ConnectionSource.instance().createConnection();
        return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public EmployeeService employeeService(){
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY HIREDATE");
                    le.clear();;
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY LASTNAME");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY SALARY");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE ORDER BY DEPARTMENT, LASTNAME");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() +" ORDER BY HIREDATE");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() +" ORDER BY SALARY");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() +" ORDER BY LASTNAME");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId().toString() +" ORDER BY LASTNAME");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId().toString() +" ORDER BY HIREDATE");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId().toString() +" ORDER BY SALARY");
                    le.clear();
                    return getEmployees(paging, list);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                try {
                    getAllDepartment();
                    getAllEmployee(true);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE ID = " + employee.getId().toString());
                    le.clear();
                    return list.get(0);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                try {
                    getAllDepartment();
                    getAllEmployee(false);
                    List<Employee> list = getEmployeesOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString() + " ORDER BY SALARY DESC");
                    le.clear();
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

    private List<Employee> getEmployeesOrder(String sql) throws SQLException {
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

    private Employee getInfoEmployee(BigInteger Mid, ResultSet resultSet, int numberChain, boolean Chain) {
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
                        manager = getInfoEmployee(BigInteger.valueOf(managerid), resultSet, numberChain+1, Chain);
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

    private void getAllEmployee(boolean Chain) throws SQLException {
        Statement ps = getStatement();
        ResultSet resultSet = ps.executeQuery("SELECT * FROM EMPLOYEE");
        while (resultSet.next()) {
            int current = resultSet.getRow();
            BigInteger id = new BigInteger(resultSet.getString("ID"));
            resultSet.beforeFirst();
            le.add(getInfoEmployee(id, resultSet, 0, Chain));
            resultSet.absolute(current);
        }
    }

    private void getAllDepartment() throws SQLException {
        Statement ps = getStatement();
        ResultSet resultSet = ps.executeQuery("SELECT * FROM DEPARTMENT");
        while (resultSet.next()) {
            ld.add(new Department(BigInteger.valueOf(resultSet.getInt("ID")), resultSet.getString("NAME"), resultSet.getString("LOCATION")));
        }
    }
}

