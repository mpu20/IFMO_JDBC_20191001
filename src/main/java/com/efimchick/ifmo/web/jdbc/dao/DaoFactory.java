package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.*;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    private PreparedStatement getPrepareStatement(String sql) {
        PreparedStatement ps = null;
        try {
            ps = ConnectionSource.instance().createConnection().prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ps;
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                return getEmployeesbyOrder("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId().toString());
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                return getEmployeesbyOrder("SELECT * FROM EMPLOYEE WHERE MANAGER = " + employee.getId().toString());
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                List<Employee> le = getEmployeesbyOrder("SELECT * FROM EMPLOYEE WHERE ID = " + Id.toString());
                if (le.isEmpty()) return Optional.empty();
                else return Optional.ofNullable(le.get(0));
            }

            @Override
            public List<Employee> getAll() {
                return getEmployeesbyOrder("SELECT * FROM EMPLOYEE");
            }

            @Override
            public Employee save(Employee employee) {
                try {
                    Optional<Employee> od = getById(employee.getId());
                    od.ifPresent(this::delete);
                    String sql = "INSERT INTO EMPLOYEE VALUES (?,?,?,?,?,?,?,?,?)";
                    PreparedStatement ps = getPrepareStatement(sql);
                    ps.setInt(1,employee.getId().intValue());
                    ps.setString(2,employee.getFullName().getFirstName());
                    ps.setString(3,employee.getFullName().getLastName());
                    ps.setString(4,employee.getFullName().getMiddleName());
                    ps.setString(5,employee.getPosition().toString());
                    ps.setInt(6,employee.getManagerId().intValue());
                    ps.setDate(7,Date.valueOf(employee.getHired()));
                    ps.setDouble(8, employee.getSalary().doubleValue());
                    ps.setInt(9,employee.getDepartmentId().intValue());
                    ps.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                try {
                    String sql = "DELETE FROM EMPLOYEE WHERE ID=?";
                    PreparedStatement ps = getPrepareStatement(sql);
                    ps.setInt(1,employee.getId().intValue());
                    ps.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private List<Employee> getEmployeesbyOrder(String sql) {
        List<Employee> le = new ArrayList<>();
        PreparedStatement ps = getPrepareStatement(sql);
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                BigInteger id  = new BigInteger(resultSet.getString("ID"));
                String first_name = resultSet.getString("FIRSTNAME");
                String middle_name = resultSet.getString("MIDDLENAME");
                String last_name = resultSet.getString("LASTNAME");
                FullName full_name = new FullName(first_name, last_name, middle_name);
                Position pos = Position.valueOf(resultSet.getString("POSITION"));
                LocalDate date_hire = LocalDate.parse(resultSet.getString("HIREDATE"));
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                BigInteger manager = new BigInteger(String.valueOf(resultSet.getInt("MANAGER")));
                BigInteger department = new BigInteger(String.valueOf(resultSet.getInt("DEPARTMENT")));
                le.add(new Employee(id, full_name, pos, date_hire, salary, manager, department));
            }
        } catch (SQLException e) {
            return null;
        }
        return le;
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                List<Department> ld = getDepartmentbyOrder("SELECT * FROM DEPARTMENT WHERE ID = " + Id.toString());
                if (ld.isEmpty()) return Optional.empty();
                else return Optional.ofNullable(ld.get(0));
            }

            @Override
            public List<Department> getAll() {
                return getDepartmentbyOrder("SELECT * FROM DEPARTMENT");
            }

            @Override
            public Department save(Department department) {
                try {
                    Optional<Department> od = getById(department.getId());
                    od.ifPresent(this::delete);
                    String sql = "INSERT INTO DEPARTMENT VALUES (?,?,?)";
                    PreparedStatement ps = getPrepareStatement(sql);
                    ps.setInt(1,department.getId().intValue());
                    ps.setString(2,department.getName());
                    ps.setString(3,department.getLocation());
                    ps.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return department;
            }

            @Override
            public void delete(Department department) {
                try {
                    String sql = "DELETE FROM DEPARTMENT WHERE ID=? ";
                    PreparedStatement ps = getPrepareStatement(sql);
                    ps.setInt(1,department.getId().intValue());
                    ps.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private List<Department> getDepartmentbyOrder(String sql) {
        List<Department> ld = new ArrayList<>();
        PreparedStatement ps = getPrepareStatement(sql);
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                BigInteger id = new BigInteger(resultSet.getString("ID"));
                String name = resultSet.getString("NAME");
                String location = resultSet.getString("LOCATION");
                ld.add(new Department(id, name, location));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ld;
    }
}
