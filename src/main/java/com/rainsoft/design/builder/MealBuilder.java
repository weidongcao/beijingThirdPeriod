package com.rainsoft.design.builder;

/**
 * Created by Administrator on 2017-08-31.
 */
public class MealBuilder {
    public Meal prepareVegMeal() {
        Meal meal = new Meal();
        meal.addItem(new VegBurger());
        meal.addItem(new Coke());

        return meal;
    }

    public Meal PrepareNonVegMeal() {
        Meal meal = new Meal();
        meal.addItem(new ChickenBurger());
        meal.addItem(new Pepsi());

        return meal;
    }
}
