package com.rainsoft.design.bridge;

/**
 * Created by Administrator on 2017-09-05.
 */
public class RedCircle implements DrawAPI {
    @Override
    public void drawCircle(int radius, int x, int y) {
        System.out.println("Drawing Circle[ color: red, radius: "
                + radius + ", x: " + x + ", y: " + y);
    }
}
