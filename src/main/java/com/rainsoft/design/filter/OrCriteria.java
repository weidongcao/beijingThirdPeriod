package com.rainsoft.design.filter;

import java.util.List;

/**
 * Created by Administrator on 2017-09-06.
 */
public class OrCriteria implements Criteria {
    private Criteria criteria;
    private Criteria otherCriteria;

    public OrCriteria(Criteria criteria, Criteria otherCriteria) {
        this.criteria = criteria;
        this.otherCriteria = otherCriteria;
    }

    @Override
    public List<Person> meetCriteria(List<Person> persons) {
        List<Person> firstCriteriaItems = criteria.meetCriteria(persons);
        List<Person> otherCriteriaItems = otherCriteria.meetCriteria(persons);
        otherCriteriaItems.stream()
                .filter(person ->
                    !firstCriteriaItems.contains(person)
                )
                .forEach(firstCriteriaItems::add);
        return firstCriteriaItems;
    }
}
