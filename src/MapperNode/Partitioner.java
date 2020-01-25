package MapperNode;

public class Partitioner {

    private static final int MAX=123;
    private static final int MIN=64;

    public static int getPartition(String line,int numOfReducer) {
        if (line==null|| numOfReducer<=1)
            throw new IllegalArgumentException();

        double total_length = MAX - MIN - 7;
        int subrange_length = (int) total_length / numOfReducer;
        double current_start = MIN;

        int[] ranges = new int[numOfReducer + 1];

        for (int i = 0; i <= numOfReducer; ++i) {
            ranges[i] = (int) current_start;
            current_start += subrange_length;
            if (current_start >= 90 && current_start < 97)
                current_start += (current_start - 90);
        }

        int reducerNum = 0;
        int uni = line.charAt(0);
        for (int i = 0; i < numOfReducer; i++) {
            if (uni >= ranges[i] && uni < ranges[i + 1]) {
                reducerNum = i + 1;

            }
        }

        return reducerNum;

    }

}
