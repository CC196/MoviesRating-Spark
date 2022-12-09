import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

class TupleComparator implements Comparator<Tuple2<Float, Integer>>,Serializable {
    public int compare(Tuple2<Float, Integer> o1, Tuple2<Float, Integer> o2){
        return -(o1._2() - o2._2());
    }
}
