package com.ahuoo.nextetl.learn.java;

public class StaticProxyTest implements ISubject{
    private ISubject subject ;

    public StaticProxyTest(){
        subject = new ISubjectImpl() ;
    }

   public  void execute(){
        System.out.println("dosomethings before");

        subject.execute();

        System.out.println("dosomethings after");
    }

    public static void main(String[] args){
        ISubject inter = new StaticProxyTest() ;
        inter.execute();
    }
}
