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
        SetMapper<Set<Employee>> set_mapper = new SetMapper<Set<Employee>>() {
            @Override
            public Set<Employee> mapSet(ResultSet resultSet) {
                try {
                    Set<Employee> se = new HashSet<>();
                    resultSet.beforeFirst();
                    while (resultSet.next()) {
                        BigInteger id = new BigInteger(resultSet.getString("ID"));
                        se.add(getInfoEmployee(id, resultSet));
                    }
                    return se;
                } catch (SQLException e) {
                    return null;
                }
            }
        };
        return set_mapper;
    }

    private Employee getInfoEmployee(BigInteger Eid, ResultSet resultSet) {
        try {
            resultSet.beforeFirst();
            while (resultSet.next()) {
                BigInteger id  = new BigInteger(resultSet.getString("ID"));
                if (Eid.equals(id)) {
                    String first_name = resultSet.getString("FIRST_NAME");
                    String middle_name = resultSet.getString("MIDDLE_NAME");
                    String last_name = resultSet.getString("LAST_NAME");
                    FullName full_name = new FullName(first_name, last_name, middle_name);
                    Position pos = Position.valueOf(resultSet.getString("POSITION"));
                    LocalDate date_hire = LocalDate.parse(resultSet.getString("HIREDATE"));
                    BigDecimal salary = resultSet.getBigDecimal("SALARY");
                    Employee manager = getInfoEmployee(new BigInteger(resultSet.getString("MANAGER")), resultSet);
                    return new Employee(id, full_name, pos, date_hire, salary, manager);
                }
            }
            return null;
        } catch (SQLException e) {
            return null;
        }

    }
}
