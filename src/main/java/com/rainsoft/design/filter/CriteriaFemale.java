package com.rainsoft.design.filter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2017-09-06.
 */
public class CriteriaFemale implements Criteria {
    @Override
    public List<Person> meetCriteria(List<Person> persons) {
        List<Person> femalePersons = persons
                .stream()
                .filter(person ->
                        person.getGender().equalsIgnoreCase("female")
                )
                .collect(Collectors.toList());
        return femalePersons;
    }
}
