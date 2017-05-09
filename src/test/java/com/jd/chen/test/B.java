package com.jd.chen.test;

/**
 * Created by chenxiaolei3 on 2017/4/14.
 */
public class B extends A {
    public B (){
        System.out.println("fdfdfd");
    }
    public static void aa1(){
        System.out.println("这是一个静态方法B");
    }

}
class C {

    public static void main(String[] args) {
        new B();
    }
}
