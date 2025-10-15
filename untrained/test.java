public class test {
}

public static void main(String[] args) {
    WordFreqInfo test = new WordFreqInfo("test", 0);
    test.updateFollows("test");
    test.updateFollows("tasty");
    System.out.println(test.getOccurCount());

    System.out.println(test);
}
