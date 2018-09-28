package org.nms.cs626.util;

import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class OrderedPair {
    /*Marking both public and final. I want the data to be immutable but using getters and setters
    seems unnecessarily verbose for this.
     */
    public final String left;
    public final String right;

    public static boolean LexicalLessOrEqual(String one, String another){
        return one.compareTo(another) <= 0;
    }

    public OrderedPair(String first, String second){
        left=first;
        right=second;
    }

    public OrderedPair reverse(){
        return new OrderedPair(right,left);
    }

    public Text asText(){
        return new Text(this.toString());
    }

    @Override
    public String toString() {
        return left+","+right;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderedPair that = (OrderedPair) o;
        return this.left.equals(that.left) && this.right.equals(that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left,right);
    }

}
