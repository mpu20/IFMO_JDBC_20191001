package com.efimchick.ifmo.web.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            try {
                Set<Employee> se = new HashSet<>();
                resultSet.beforeFirst();
                while (resultSet.next()) {
                    BigInteger id = new BigInteger(resultSet.getString("ID"));
                    int current = resultSet.getRow();
                    se.add(getInfoEmployee(id, resultSet));
                    resultSet.absolute(current);
                }
                return se;
            } catch (SQLException e) {
                return null;
            }
        };
    }

    private Employee getInfoEmployee(BigInteger Mid, ResultSet resultSet) {
        try {
            resultSet.beforeFirst();
            while (resultSet.next()) {
                BigInteger id  = new BigInteger(resultSet.getString("ID"));
                if (Mid.equals(id)) {
                    String first_name = resultSet.getString("FIRSTNAME");
                    String middle_name = resultSet.getString("MIDDLENAME");
                    String last_name = resultSet.getString("LASTNAME");
                    FullName full_name = new FullName(first_name, last_name, middle_name);
                    Position pos = Position.valueOf(resultSet.getString("POSITION"));
                    LocalDate date_hire = LocalDate.parse(resultSet.getString("HIREDATE"));
                    BigDecimal salary = resultSet.getBigDecimal("SALARY");
                    String smanager = resultSet.getString("MANAGER");
                    Employee manager = null;
                    if (smanager != null) {
                        int current = resultSet.getRow();
                        manager = getInfoEmployee(new BigInteger(smanager), resultSet);
                        resultSet.absolute(current);
                    }
                    return new Employee(id, full_name, pos, date_hire, salary, manager);
                }
            }
            return null;
        } catch (SQLException e) {
            return null;
        }

    }
}
