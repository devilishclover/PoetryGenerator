import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.List;
import java.util.HashSet;

public class WritePoetry {
    private static final String CACHE_FILE = "hashtable_cache.ser";
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int MAX_ATTEMPTS = 50; // Max attempts to find a non-repeating word
    
    public String writePoem(String file, String startWord, int length, boolean printHashtable) {
        String currentWord = startWord;
        HashTable<String, WordFreqInfo> table = table(file);
        StringBuilder returnString = new StringBuilder();
        HashSet<String> usedWords = new HashSet<>();
        
        System.out.println("\nGenerating poem...");
        ProgressBar poemProgress = new ProgressBar("Writing", length);
        
        for (int i = 0; i < length; i++) {
            Random rnd = new Random();
            WordFreqInfo wordInfo = table.find(currentWord);
            
            if (wordInfo == null) {
                System.out.println("\nWarning: Word '" + currentWord + "' not found in hash table. Stopping generation.");
                break;
            }
            
            String nextWord = null;
            int attempts = 0;
            
            // Try to find a word that hasn't been used yet
            while (attempts < MAX_ATTEMPTS) {
                int count = rnd.nextInt(wordInfo.getOccurCount());
                String candidate = wordInfo.getFollowWord(count);
                
                // Check if it's not a repeat (or if it's punctuation, allow it)
                if (!usedWords.contains(candidate) || 
                    candidate.equals(".") || candidate.equals(",") || 
                    candidate.equals("!") || candidate.equals("?") ||
                    candidate.equals("\n")) {
                    nextWord = candidate;
                    break;
                }
                attempts++;
            }
            
            // If we couldn't find a non-repeating word after MAX_ATTEMPTS, use any word
            if (nextWord == null) {
                int count = rnd.nextInt(wordInfo.getOccurCount());
                nextWord = wordInfo.getFollowWord(count);
            }
            
            // Check if current word is punctuation
            boolean isPunctuation = currentWord.equals(".") || currentWord.equals(",") || 
                                   currentWord.equals("!") || currentWord.equals("?");
            
            // Check if next word is punctuation
            boolean nextIsPunctuation = nextWord.equals(".") || nextWord.equals(",") || 
                                       nextWord.equals("!") || nextWord.equals("?");
            
            // Add current word to output
            if (isPunctuation) {
                // Punctuation: no space before, add space after (unless it's the last word)
                returnString.append(currentWord);
                if (i < length - 1 && !nextIsPunctuation) {
                    returnString.append(" ");
                }
            } else {
                // Regular word: add space after only if next word is not punctuation
                returnString.append(currentWord);
                if (!nextIsPunctuation && i < length - 1) {
                    returnString.append(" ");
                }
            }
            
            // Track used words (but not punctuation)
            if (!isPunctuation && !currentWord.equals("\n")) {
                usedWords.add(currentWord);
            }

            currentWord = nextWord;
            poemProgress.increment();
        }
        poemProgress.finish();
        return returnString.toString();
    }

    public HashTable<String, WordFreqInfo> table(String file){
        // Try to load cached hash table
        File cacheFile = new File(CACHE_FILE);
        if (cacheFile.exists()) {
            System.out.println("Found cached hash table (" + (cacheFile.length() / 1024 / 1024) + " MB). Loading...");
            System.out.println("This may take a minute or two for large files...");
            HashTable<String, WordFreqInfo> loadedTable = loadHashTable();
            if (loadedTable != null) {
                System.out.println("Successfully loaded hash table with " + loadedTable.size() + " unique words!\n");
                return loadedTable;
            }
            System.out.println("Failed to load cache. Building new hash table...");
        } else {
            System.out.println("No cached hash table found. Building from scratch...");
        }
        
        ArrayList<String> wordList = WordList(file);
        
        System.out.println("Building hash table from " + wordList.size() + " words...");
        long startTime = System.currentTimeMillis();
        
        HashTable<String, WordFreqInfo> table = buildHashTableOptimized(wordList);
        
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Hash table built with " + table.size() + " unique words in " + (elapsed/1000) + "s");
        
        // Save the hash table for next time
        System.out.println("Saving hash table to cache...");
        if (saveHashTable(table)) {
            System.out.println("Hash table saved successfully!\n");
        } else {
            System.out.println("Failed to save hash table.\n");
        }
        
        return table;
    }
    
    private boolean saveHashTable(HashTable<String, WordFreqInfo> table) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(CACHE_FILE))) {
            oos.writeObject(table);
            return true;
        } catch (IOException ex) {
            System.out.println("Error saving hash table: " + ex);
            return false;
        }
    }
    
    @SuppressWarnings("unchecked")
    private HashTable<String, WordFreqInfo> loadHashTable() {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(CACHE_FILE))) {
            return (HashTable<String, WordFreqInfo>) ois.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            System.out.println("Error loading hash table: " + ex);
            return null;
        }
    }
    
    private HashTable<String, WordFreqInfo> buildHashTableOptimized(ArrayList<String> wordList) {
        HashTable<String, WordFreqInfo> table = new HashTable<>(wordList.size() / 10); // Pre-size to reduce rehashing
        
        ProgressBar hashProgress = new ProgressBar("Hashing", wordList.size() - 1);
        
        for (int i = 0; i < wordList.size() - 1; i++) {
            String word = wordList.get(i);
            String nextWord = wordList.get(i + 1);
            
            WordFreqInfo info = table.find(word);
            if (info != null) {
                info.updateFollows(nextWord);
            } else {
                table.insert(word, new WordFreqInfo(word, 0));
                table.find(word).updateFollows(nextWord);
            }
            
            hashProgress.increment();
        }
        hashProgress.finish();
        
        return table;
    }

    public ArrayList<String> WordList(String file){
        File poem = new File(file);
        ArrayList<String> wordList= new ArrayList<>();
        
        // First pass: count lines for progress bar
        int totalLines = 0;
        try (BufferedReader counter = new BufferedReader(new FileReader(poem), 8192 * 4)) {
            while (counter.readLine() != null) {
                totalLines++;
            }
        } catch (java.io.IOException ex) {
            System.out.println("An error occurred trying to count lines: " + ex);
        }
        
        System.out.println("Reading and parsing file (" + totalLines + " lines)...");
        ProgressBar readProgress = new ProgressBar("Reading", totalLines);
        
        try (BufferedReader reader = new BufferedReader(new FileReader(poem), 8192 * 4)) {
            String line;
            
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("[ ]");

                for (String token : tokens){
                    if (token.length() == 0) continue;
                    
                    String word = token.toLowerCase();
                    
                    // Handle punctuation - extract word and punctuation separately
                    if (word.matches(".*[.,!?].*")) {
                        String cleanWord = word.replaceAll("[.,!?]", "");
                        if (cleanWord.length() > 0) {
                            wordList.add(cleanWord);
                        }
                        // Extract punctuation marks
                        for (char c : word.toCharArray()) {
                            if (c == '.' || c == ',' || c == '!' || c == '?') {
                                wordList.add(String.valueOf(c));
                            }
                        }
                    } else {
                        wordList.add(word);
                    }
                }
                readProgress.increment();
            }
        } catch (java.io.IOException ex) {
            System.out.println("An error occurred trying to read the dictionary: " + ex);
        }
        
        readProgress.finish();
        System.out.println("Parsed " + wordList.size() + " words from file.\n");
        return wordList;
    }
}
