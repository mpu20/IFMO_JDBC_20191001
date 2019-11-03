package com.efimchick.ifmo.web.jdbc;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class RowMapperFactory {

    public RowMapper<Employee> employeeRowMapper() {
        RowMapper<Employee> row_mapper = new RowMapper<Employee>() {
            @Override
            public Employee mapRow(ResultSet resultSet) {
                try {
                    BigInteger id = new BigInteger(resultSet.getString("ID"));
                    String first_name = resultSet.getString("FIRSTNAME");
                    String middle_name = resultSet.getString("MIDDLENAME");
                    String last_name = resultSet.getString("LASTNAME");
                    FullName full_name = new FullName(first_name, last_name, middle_name);
                    Position pos = Position.valueOf(resultSet.getString("POSITIOn"));
                    LocalDate hire_date = LocalDate.parse(resultSet.getString("HIREDATE"));
                    BigDecimal salary = resultSet.getBigDecimal("SALARY");
                    return new Employee(id, full_name, pos, hire_date, salary);
                } catch (SQLException e) {
                    return null;
                }
            }
        };
        return row_mapper;
    }
}
