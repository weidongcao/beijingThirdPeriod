package com.rainsoft.design.builder;

/**
 * Created by Administrator on 2017-08-31.
 */
public class BuiderPatternDemo {
    public static void main(String[] args) {
        MealBuilder builder = new MealBuilder();

        Meal vegMeal = builder.prepareVegMeal();
        System.out.println("Veg Meal");
        vegMeal.showItems();
        System.out.println("Total cost: " + vegMeal.getCost());

        Meal noVegMeal = builder.PrepareNonVegMeal();
        System.out.println("\n\nNon-Veg meal");
        noVegMeal.showItems();
        System.out.println("total Cost: " + noVegMeal.getCost());
    }

}
