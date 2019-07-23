package com.ahuoo.nextetl;

public class WaitingJob {
    public static void main(String[] args){
        System.out.println("Hello World!!");
        for(String str : args){
            System.out.println("Arg: " + str);
        }
        String sec = args[1];
        System.out.println("Begin to sleep "+ sec +" seconds");
        try {
            Thread.sleep(Long.valueOf(sec));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
