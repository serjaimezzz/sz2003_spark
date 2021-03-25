package _01ReadFile;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class _03JavaIODemo {
    public static void main(String[] args) {
        FileOutputStream fos = null;
        try {
             fos = new FileOutputStream("output/sc.txt", true);
            int count = 0;
            while (count <= 10){
                fos.write(("hello"+System.currentTimeMillis()).getBytes());
                Thread.sleep(1000);
                count ++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            try {
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
