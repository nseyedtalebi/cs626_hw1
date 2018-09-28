package org.nms.cs626;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.nms.cs626.util.OrderedPair;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class AppTest {
    @Mock
    private Mapper.Context mockMapperContext;
    @Mock
    private Reducer.Context mockReducerContext;
    @Mock
    private Counter mockCounter;

    private App myApp;
    private App.Map myMap;
    private App.Reduce myReduce;
    //Found useful example of how to mock mapreduce stuff here:
    //https://www.baeldung.com/mockito-argument-matchers
    private ArgumentCaptor<Text> textcaptor = ArgumentCaptor.forClass(Text.class);
    private ArgumentCaptor<IntWritable> intWritableCaptor = ArgumentCaptor.forClass(IntWritable.class);
    private static OrderedPair testPair = new OrderedPair("C1585558","C1699312");
    private static IntWritable zero = new IntWritable(0);
    private static IntWritable one = new IntWritable(1);
    private static IntWritable two = new IntWritable(2);
    private static IntWritable three = new IntWritable(3);


    @BeforeEach
    public void init() throws IOException, InterruptedException {
        initMocks(this);
        myApp = new App();
        myMap = new App.Map();
        myReduce = new App.Reduce();

        doNothing().when(mockMapperContext).write(textcaptor.capture(),intWritableCaptor.capture());
        doNothing().when(mockReducerContext).write(textcaptor.capture(), intWritableCaptor.capture());
        doReturn(mockCounter).when(mockReducerContext).getCounter(App.Reduce.CountersEnum.class.getName()
        ,App.Reduce.CountersEnum.UNIQUE_OUTPUT_PAIRS.toString());
        doNothing().when(mockCounter).increment(anyLong());
    }
    public static Stream<Arguments> getMapTestArgs(){
        //LongWritable offset, Text lineText, Mapper.Context context
        return Stream.of(
            Arguments.of("\"C1699312\",\"C1585558\"",testPair.asText()),
            Arguments.of("\"C1585558\",\"C1699312\"",testPair.asText()),
            Arguments.of("C1585558,C1699312",testPair.asText())
        );
    }

    @ParameterizedTest
    @MethodSource("getMapTestArgs")
    public void mapTest(String inputLine,Text expected) throws IOException, InterruptedException{
        myMap.map(new LongWritable(0),new Text(inputLine), mockMapperContext);
        assertEquals(expected, textcaptor.getValue());
        assertEquals(one,intWritableCaptor.getValue());
    }

    public static Stream<Arguments> getReduceTestArgs(){
        return Stream.of(
                Arguments.of(testPair.asText(),Arrays.asList(one),one),
                Arguments.of(testPair.asText(),Arrays.asList(one,two),three),
                Arguments.of(testPair.reverse().asText(),Arrays.asList(one),one)
        );
    }

    @ParameterizedTest
    @MethodSource("getReduceTestArgs")
    public void reduceTest(Text keyin, Iterable<IntWritable> valuesIn,IntWritable expectedOutput)
    throws IOException, InterruptedException {
        myReduce.reduce(keyin, valuesIn, mockReducerContext);
        assertEquals(expectedOutput,intWritableCaptor.getValue());
        assertEquals(keyin, textcaptor.getValue());
    }

    @ParameterizedTest
    @MethodSource("getReduceTestArgs")
    public void reduceCountersTest(Text keyin, Iterable<IntWritable> valuesIn)
    throws IOException, InterruptedException {
        myReduce.reduce(keyin, valuesIn, mockReducerContext);
        verify(mockCounter,times(1)).increment(1);
    }

    @Test
    public void reduceStep(){
        List<IntWritable> ar = Arrays.asList(one,two,three);
        int theSum = StreamSupport.stream(ar.spliterator(),false).map(IntWritable::get).reduce(0,(a, b)->a+b);
        assertEquals(6,theSum);
    }
}
