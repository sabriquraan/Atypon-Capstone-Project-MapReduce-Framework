package ReducerNode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Preparator {

    Preparator(){
        throw new AssertionError();
    }
    public static Map<String, List<String>> prepareInput(int reducerNum) throws IOException {


        TreeMap<String, List<String>> reducer = new TreeMap<String, List<String>>();
        try (
                BufferedReader src = new BufferedReader(new FileReader("/dirc/Files/reducer/"+reducerNum+"reducer.txt"));
        ) {

            String str = src.readLine();
            while (str != null) {
                String[] s = str.split(":");
                reducer.computeIfAbsent(s[0], k -> new ArrayList<>()).add(s[1]);
                str = src.readLine();
            }
        }

        return reducer;
    }
}
