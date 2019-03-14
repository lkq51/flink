package test.java.stream;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * @Author lou
 */
public class CollectionStream {
	List<String> strs  = Arrays.asList("11212", "dfd", "2323", "dfhgf");

	Stream<String> stream = strs.stream();
	Stream<String> stream1 = strs.parallelStream();

	@Test
	public void testFilter(){
		Integer[] arr  = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
		Arrays.stream(arr).filter(x -> x > 3 && x < 8).forEach(System.out :: println);
	}
}