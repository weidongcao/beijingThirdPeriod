package com.rainsoft.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 1、配置管理组件可以复杂，也可以很简单，对于简单的配置管理组件来说，只要开发一个类，可
 * 以在第一次访问它的时候就从对应折properties文件中读取配置项，并提供外界获取某个配置key
 * 对应的value的方法
 * <p>
 * 2、如果是特别复杂的配置管理组件，那么可能需要使用一些软件设计中的设计模式，比如单例，解释器模式
 * 可能需要管理多个不同的properties，甚至是xml类型的配置文件
 * <p>
 * 3、我们这里的话就是开发一个简单的配置管理组件就可以了
 * <p>
 * Created by caoweidong on 2017/2/5.
 *
 * @author caoweidong
 */
public class ConfigurationManager {

    //Properties对象
    private static Properties prop = new Properties();

    static {
        try {
            //通过一个类名.class的方式，就可以获取到这个类在JVM中对应的Class对象
            //然后再通过这个Class对象的getClassLoader()方法，就可以获取到当初加载这个类的JVM中的类加载器（ClassLoader）
            //然后调用ClassLoader的getResourceAsStream()这个方法就可以用类加载器去加载类加载路径中指定的文件
            //最终可以获取到一个，针对指定文件的输入流（InputStream）
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("application.properties");

            //调用Properties的load()方法，给它传入一个文件的InputStream输入流
            //即可将文件中的符合“key=value”格式的配置项都加载到Properies对象中
            // 加载过后，此时Properties对象中就有了配置文件中所有的Key-value对了
            prop.load(in);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key
     * @return
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key property
     * @return true & false
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
