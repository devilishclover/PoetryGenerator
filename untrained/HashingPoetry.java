import java.io.File;
import java.util.Scanner;

public class HashingPoetry {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        WritePoetry poem = new WritePoetry();
        
        System.out.println("=== Poetry Generator ===\n");
        
        System.out.print("Enter starting word: ");
        String startWord = scanner.nextLine().trim();
        
        System.out.print("Enter poem length (number of words): ");
        int length = scanner.nextInt();
        
        System.out.println("\n--- Generated Poem ---");
        System.out.println(poem.writePoem("data/combined_cleaned.txt", startWord, length, false));
        System.out.println();
        
        scanner.close();
    }
}
