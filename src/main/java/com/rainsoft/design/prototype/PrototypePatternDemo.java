package com.rainsoft.design.prototype;

/**
 * 原型模式批在创建重复对象的同时保持性能。这种类型的设计模式属于创建模式，
 * 因为此模式提供了创建对象的最佳方法之王。
 * 这个模式涉及实现一个原型接口，它只创建当前对象的克隆。有时直接创建对象时
 * 使用这种模式是昂贵的。
 * 例如：在昂贵的数据库操作之后创建对象。因此我们可以缓存对象，在下一个请求时
 * 返回其克隆，并在需要时更新数据库，从而减少数据库调用
 * Created by Administrator on 2017-09-01.
 */
public class PrototypePatternDemo {
    public static void main(String[] args) {
        ShapeCache.loadCache();

        Shape cloneShape = ShapeCache.getShape("1");
        System.out.println("Shape: " + cloneShape.getType());

        Shape cloneShape2 = ShapeCache.getShape("2");
        System.out.println("Shape: " + cloneShape2 .getType());

        Shape cloneShape3 = ShapeCache.getShape("3");
        System.out.println("Shape: " + cloneShape3.getType());

    }
}
