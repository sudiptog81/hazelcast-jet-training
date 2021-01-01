/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import dto.Trade;
import sources.TradeSource;

public class Lab5 {


    public static void main(String[] args) {
        Pipeline p = buildPipeline();

        JetInstance jet = Jet.bootstrappedInstance();

        try {
            Job job = jet.newJob(p);
            job.join();
        } finally {
            jet.shutdown();
        }
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.readFrom(TradeSource.tradeSource(1000))
                .withNativeTimestamps(0)

                // STEP 1 - Compute sum of trades for 3-second intervals
                // - Use 3 sec tumbling windows (defined in WindowDef.tumbling with size 3000
                // - Sum trade prices
                // Run the job and inspect the results. Stop the Job before moving to STEP 2.

                /* After Step 1:
                .window(WindowDefinition.tumbling(3000))
                .aggregate(AggregateOperations.summingLong(Trade::getPrice))
                 */

                // STEP 2 - Compute sum of trades for 3-second intervals with speculative results every second
                // - Use early results when defining the window
                // - Watch the early result flag in the console output
                // Run the job and inspect the results. Stop the Job before moving to STEP 3.

                /* After Step 2:
                .window(WindowDefinition.tumbling(3000).setEarlyResultsPeriod(1000))
                .aggregate(AggregateOperations.summingLong(Trade::getPrice))
                 */

                // STEP 3 - Compute sum of trades in last 3-second, updated each second
                // - Use 3 sec sliding windows with 1 sec step
                // Run the job and inspect the results. Stop the Job before moving to STEP 4.

                /* After Step 3:
                .window(WindowDefinition.sliding(3000, 1000))
                .aggregate(AggregateOperations.summingLong(Trade::getPrice))
                 */

                // STEP 4 - Compute sum of trades in last 3-second for each trading symbol
                // - Group the stream on the trading symbol
                // - Use 3 sec sliding windows with 1 sec step
                // Run the job and inspect the results. Stop the Job before leaving the lab.

                .groupingKey(Trade::getSymbol)
                .window(WindowDefinition.sliding(3000, 1000))
                .aggregate(AggregateOperations.summingLong(Trade::getPrice))

                .writeTo(Sinks.logger());


        return p;
    }
}
