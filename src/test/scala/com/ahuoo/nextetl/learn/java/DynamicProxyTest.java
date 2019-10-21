package com.ahuoo.nextetl.learn.java;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.ProxyGenerator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * JDK 动态代理
 * 从静态代理中可以看出: 静态代理只能代理一个具体的类，如果要代理一个接口的多个实现的话需要定义不同的代理类。
 *
 * 需要解决这个问题就可以用到 JDK 的动态代理。
 *
 * 其中有两个非常核心的类:
 *
 * java.lang.reflect.Proxy类。
 * java.lang.reflect.InvocationHandler接口。
 * Proxy 类是用于创建代理对象，而 InvocationHandler 接口主要你是来处理执行逻辑。
 *
 * SLF4J(Simple logging Facade for Java)是java的一个日志门面，实现了日志框架一些通用的api，log4j和logback是具体的日志框架。
 */
public class DynamicProxyTest implements InvocationHandler {
    private final static Logger log = LoggerFactory.getLogger(DynamicProxyTest.class);

    private Object target;


    public DynamicProxyTest(Class clazz) {
        try {
            this.target = clazz.newInstance();
        } catch (InstantiationException e) {
            log.error("InstantiationException", e);
        } catch (IllegalAccessException e) {
            log.error("IllegalAccessException",e);
        }
    }

    public static void  main(String[] args){
        //动态代理调用
        DynamicProxyTest handle = new DynamicProxyTest(ISubjectImpl.class) ;
        ISubject subject = (ISubject) Proxy.newProxyInstance(DynamicProxyTest.class.getClassLoader(), new Class[]{ISubject.class}, handle);
        subject.execute() ;

        //生成代理类
        byte[] proxyClassFile = ProxyGenerator.generateProxyClass("$Proxy1", new Class[]{ISubject.class}, 1);
        try {
            FileOutputStream out = new FileOutputStream("$Proxy1.class") ;
            out.write(proxyClassFile);
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        before();
        Object result = method.invoke(target, args);
        after();

        log.info("proxy class={}", proxy.getClass());
        return result;
    }


    private void before() {
        log.info("handle before");
    }

    private void after() {
        log.info("handle after");
    }


}