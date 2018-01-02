package com.rainsoft.design.filter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2017-09-06.
 */
public class CriteriaSingle implements Criteria {
    @Override
    public List<Person> meetCriteria(List<Person> persons) {
        List<Person> singlePersons = persons
                .stream()
                .filter(person ->
                        person.getMaritalStatus()
                                .equalsIgnoreCase("single")
                )
                .collect(Collectors.toList());
        return singlePersons;
    }
}
