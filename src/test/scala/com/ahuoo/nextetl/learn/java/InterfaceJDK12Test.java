package com.ahuoo.nextetl.learn.java;

public class InterfaceJDK12Test implements InterfaceJDK12 {
    // implementing abstract method
    public void existingMethod(String str){
        System.out.println("String is: "+str);
    }
    public static void main(String[] args) {
        InterfaceJDK12Test obj = new InterfaceJDK12Test();

        //calling the abstract method of interface
        obj.existingMethod("Java 8 is easy to learn");

        //calling the default method of interface
        obj.newMethod();

        //calling the static method of interface
        InterfaceJDK12.anotherNewMethod();
    }
}