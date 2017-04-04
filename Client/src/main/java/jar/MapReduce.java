package jar;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {

    public static void main(String[] args) throws IOException {


        Scanner scanner = new Scanner(System.in);
        int threads;
        do {
            System.out.println("Enter the number of threads to use:");
            threads = scanner.nextInt();

        } while (threads < 1);


        String dir = "C:\\mapred\\";
        String[] files = {"file1.txt", "file2.txt", "file3.txt"};

        Map<String, String> input = new HashMap<>();

        for (String file : files) {
            byte[] bytes = Files.readAllBytes(Paths.get(dir + file));
            input.put(file, new String(bytes));
        }

        long startTime = System.currentTimeMillis();
        distMapReduce(input, threads);
        long endTime = System.currentTimeMillis();

        System.out.println("\nThat took " + (endTime - startTime) + " milliseconds");
    }


    // APPROACH #3: Distributed MapReduce
    public static void distMapReduce(Map input, int num_threads) {

        final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

        // MAP:
        final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

        // map callback associates keys with lists of 'results'
        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        // HashSet iterator? Order not guaranteed
        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();

        ExecutorService executor = Executors.newFixedThreadPool(num_threads);

        // start a thread to map each file
        while (inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            final String file = entry.getKey();
            final String contents = entry.getValue();

            executor.execute(() -> map(file, contents, mapCallback));

        }

        executor.shutdown();
        while (!executor.isTerminated()) ;

        // GROUP:
        Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

        Iterator<MappedItem> mappedIter = mappedItems.iterator();
        while (mappedIter.hasNext()) {

            MappedItem item = mappedIter.next();

            String word = item.getWord();
            String file = item.getFile();

            List<String> list = groupedItems.get(word);
            if (list == null) {
                list = new LinkedList<String>();
                groupedItems.put(word, list);
            }

            list.add(file);
        }

        //System.out.println(groupedItems);

        // REDUCE:
        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };

        List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

        executor = Executors.newFixedThreadPool(num_threads);

        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
        while (groupedIter.hasNext()) {

            Map.Entry<String, List<String>> entry = groupedIter.next();
            final String word = entry.getKey();
            final List<String> list = entry.getValue();

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    reduce(word, list, reduceCallback);
                }
            });
        }

        executor.shutdown();
        while (!executor.isTerminated()) ;

        System.out.println(output);

    }


    public static interface MapCallback<E, V> {
        public void mapDone(E key, List<V> values);
    }


    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        // map the capitalised first letter of the word
        for (String word : words) {
            results.add(new MappedItem(word.substring(0, 1).toUpperCase(), file));
        }
        callback.mapDone(file, results);
    }


    public static interface ReduceCallback<E, K, V> {
        public void reduceDone(E e, Map<K, V> results);
    }


    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }


    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

}
