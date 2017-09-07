package com.rainsoft.design.filter;

import java.util.List;

/**
 * Created by Administrator on 2017-09-06.
 */
public interface Criteria {
    List<Person> meetCriteria(List<Person> persons);
}

