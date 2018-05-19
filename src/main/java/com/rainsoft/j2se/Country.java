package com.rainsoft.j2se;

// import javax.naming.Name;

/**
 * Created by CaoWeiDong on 2018-03-19.
 */
public class Country {
    String name;
    long population;

    public Country(String name, long population) {
        super();
        this.name = name;
        this.population = population;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public long getPopulation() {
        return population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    @Override
    public int hashCode() {
        if (this.name.length() % 2 == 0) {
            return 31;
        } else {
            return 95;
        }
    }

    @Override
    public boolean equals(Object object) {
        Country other = (Country) object;
        if (name.equalsIgnoreCase(other.name)) {
            return true;
        }
        return false;
    }
}
