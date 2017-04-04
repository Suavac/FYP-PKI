package jar;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

/**
 * Created by Suavek on 04/04/2017.
 */
public class RegisterUsers {
    public static void main(String... args) {

        while (true) {
            System.out.println("----- REGISTER NEW USER -----");
            try (FileWriter fw = new FileWriter("users.csv", true);
                 BufferedWriter bw = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(bw)) {
                out.println(getUserCredentials());
                System.out.println("------------ OK -------------");
                System.out.println("");
            } catch (IOException e) {
                //exception handling left as an exercise for the reader
            }
        }


    }

    public static String getUserCredentials() {
        System.out.print("Please enter your user name: ");
        String name = new Scanner(System.in).nextLine();
        System.out.print("Please enter your password: ");
        String password = new Scanner(System.in).nextLine();
        return new String(name + "," + password);
    }
}
