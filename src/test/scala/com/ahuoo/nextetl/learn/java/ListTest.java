package com.ahuoo.nextetl.learn.java;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * implements Externalizable if you want serialize a object.
 */
public class ListTest implements Externalizable {
    private Integer id = 123;

    private static AtomicInteger count = new AtomicInteger(10);

    final public static void main(String[] args){
        List list = new ArrayList<Integer>();
        list.add(0,1);
        list.add(1,2);
        System.out.println(list);
        list.add(1,3);
        System.out.println(list);

        {
            class LocalClass {
                private String name = "test";
            }
            LocalClass cls = new LocalClass();
        }

        synchronized (ListTest.class){
            ListTest lt = new ListTest();
            lt.fun();
            count.getAndIncrement();
        }
        System.out.println(count.incrementAndGet());

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    static class StaticNestedClass{
        private String name = "test";
    }

    private void fun(){
        MemeberClass mc = new MemeberClass();
        mc.mcFun();
    }

    public class MemeberClass{
        private String name = "test";
        private void mcFun(){
            System.out.println(id);
        }

    }


}
