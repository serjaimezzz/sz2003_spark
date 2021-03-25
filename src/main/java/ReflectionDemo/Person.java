package ReflectionDemo;

public class Person {
    String name;
    int age;
}

class ptest{
    public static void main(String[] args) {
        Class<?> person = null;
        try {
            person = Class.forName("ReflectionDemo.Person");
            System.out.println(person.getName());
            System.out.println(person.getConstructor());
            System.out.println(person.getConstructors());   //获取公有构造
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}