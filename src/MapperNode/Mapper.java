package MapperNode;

import java.io.BufferedReader;
import java.util.Map;

public interface Mapper {
      Map<?,?> mapper(BufferedReader source);
}
