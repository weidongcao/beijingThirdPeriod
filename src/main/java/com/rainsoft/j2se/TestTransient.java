package com.rainsoft.j2se;

import java.io.*;

/**
 * 测试transient的使用
 * 被transient修饰的变量有以下特点：
 * 1. 一旦变量被transient修饰，变量将不再是对象持久化的一部分，该变量内容在序列化后无法获得访问。
 * 2. transient关键字只能修饰变量，而不能修饰方法和类。注意，本地变量是不能被transient关键字修饰的。
 *    变量如果是用户自定义类变量，则该类需要实现Serializable接口。
 * 3. 被transient关键字的变量不再能被序列化，一个静态变量不管是否被transient修饰，均不能被序列化。
 * Created by CaoWeiDong on 2018-08-29.
 */
public class TestTransient {
    public static void main(String[] args) {
        createSerialize();

        readSerialize();
    }

    public static void readSerialize() {
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream("user.txt"))){
            //在反序列化之前改变name值
            // UserInfo.setName("hello");
            //重新读取内容
            UserInfo readUser = (UserInfo) ois.readObject();
            //读取后psw的内容为null
            System.out.println("readUser = " + readUser);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void createSerialize() {
        UserInfo user = new UserInfo("张三", "123567");
        System.out.println("user = " + user);
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("user.txt"))){
            oos.writeObject(user);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class UserInfo implements Serializable{

    private static final long serialVersionUID = -114132318653810227L;
    private static String name;
    private transient String psw;

    public UserInfo(String name, String psw) {
        this.name = name;
        this.psw = psw;
    }
    public static String getName() {
        return name;
    }

    public static void setName(String name) {
        UserInfo.name = name;
    }
    public String getPsw() {
        return psw;
    }

    public void setPsw(String psw) {
        this.psw = psw;
    }

    @Override
    public String toString() {
        return "[ name = '" + name + "', psw = '" + psw + "']";
    }
}