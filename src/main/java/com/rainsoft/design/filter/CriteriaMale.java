package com.rainsoft.design.filter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by CaoWeiDong on 2017-09-06.
 */
public class CriteriaMale implements Criteria {
    @Override
    public List<Person> meetCriteria(List<Person> persons) {
        List<Person> malePersons = persons
                .stream()
                .filter(person ->
                        person.getGender().equalsIgnoreCase("male")
                ).collect(Collectors.toList());

        return malePersons;
    }
}
