package com.rainsoft.design.bridge;

/**
 * Created by Administrator on 2017-09-05.
 */
public abstract class Shape {
     DrawAPI drawAPI;

     Shape(DrawAPI drawAPI) {
          this.drawAPI = drawAPI;
     }

     public abstract void draw();
}
