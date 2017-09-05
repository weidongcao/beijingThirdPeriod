package com.rainsoft.design.prototype;

import java.util.Hashtable;

/**
 *我们将创建一个抽象类Shape和扩展Shape类的具体类。在下一步中定义ShapeCache类，
 * 在HashTable中存储一个形状(Shape)对象，并在请求时返回其克隆
 * PrototypPatternDemo这是一个演示类，将使用ShapeCache类来获取一个Shape对象。
 *
 * Created by Administrator on 2017-09-01.
 */
public class ShapeCache {
    private static Hashtable<String, Shape> shapeMap = new Hashtable<>();

    public static Shape getShape(String shapeId) {
        Shape cachedShape = shapeMap.get(shapeId);
        return (Shape) cachedShape.clone();
    }
    //for each shape run database query and create shape
    //shapeMap.put(shapeKey,Shape);
    //for example, we are adding three shapes
    public static void loadCache() {
        Circle circle = new Circle();
        circle.setId("1");
        shapeMap.put(circle.getId(), circle);

        Square square = new Square();
        square.setId("2");
        shapeMap.put(square.getId(), square);

        Rectangle rectangle = new Rectangle();
        rectangle.setId("3");
        shapeMap.put(rectangle.getId(), rectangle);
    }
}
