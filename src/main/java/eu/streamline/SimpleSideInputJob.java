/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.streamline;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.util.SideInput;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class SimpleSideInputJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);


        ParameterTool params = ParameterTool.fromArgs(args);
        String streamPath = params.get("stream-input");
        String sideInputPath = params.get("side-input");


        // prepare the realtime stream
        DataStream<Tuple2<Integer, String>> realTimeStream = createStreamSource(streamPath, env);

        // prepare the stream for sideinput
        DataStream<Tuple2<Integer, String>> sideSource = createStreamSource(sideInputPath, env);

        // create the sideinput object
        final SideInput<Tuple2<Integer, String>> sideInput = env.newKeyedSideInput(sideSource, 0);

        // joining the realtime stream with the batch data
        DataStream<Tuple2<Integer, String>> joinedWithBatchStream = realTimeStream.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {


                    @Override
                    public void flatMap(Tuple2<Integer, String> value, Collector<Tuple2<Integer, String>> collector) throws Exception {
                        // access the content of the side input object
                        Iterable<Tuple2<Integer, String>> batchData = getRuntimeContext().getSideInput(sideInput);
                        if (!isLoaded) {
                            for (Tuple2<Integer, String> e : batchData) {
                                joinTable.put(e.f0, e.f1);
                            }
                            isLoaded = true;
                        }
                        Integer key = value.f0;
                        if (joinTable.contains(key)) {
                            joinTable.put(key, joinTable.get(key) + "," + value.f1);
                        } else {
                            joinTable.put(key, value.f1);
                        }
                        collector.collect(new Tuple2<>(key, joinTable.get(key)));
                    }

                    private MapState<Integer, String> joinTable;
                    private boolean isLoaded = false;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        MapStateDescriptor mapDescriptor =
                                new MapStateDescriptor<>("join-table", Integer.class, String.class);
                        joinTable = getRuntimeContext().getMapState(mapDescriptor);
                    }

                }).withSideInput(sideInput);

        if (params.has("output")){
            joinedWithBatchStream.writeAsText((params.get("output")));
        } else {
            joinedWithBatchStream.print();
        }

        // execute program
        env.execute("Simple Streamline Job");
    }

    private static DataStream<Tuple2<Integer, String>> createStreamSource(String streamPath, StreamExecutionEnvironment env) {
        return env.readTextFile(streamPath).map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String s) throws Exception {
                String[] values = s.split(",");
                return new Tuple2<>(Integer.parseInt(values[0]), values[1]);
            }
        });
    }
}

