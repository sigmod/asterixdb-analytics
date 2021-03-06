/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.example.kmeans;

import java.io.Serializable;
import java.util.Random;

public class KMeansStartingPoints implements Serializable {
    DataPoint[] ps;
    int n = 0;
    Random random = new Random();

    public KMeansStartingPoints(int k) {
        ps = new DataPoint[k];
    }

    public void add(DataPoint dp) {
        if (n < ps.length) {
            ps[n] = dp;
        } else {
            int position = random.nextInt(n + 1);
            if (position < ps.length)
                ps[position] = dp;
        }
        n++;
    }

    public void add(KMeansStartingPoints result) {
        for (int i = 0; i < ps.length; i++) {
            if (random.nextInt(n + result.n) >= n)
                ps[i] = result.ps[i];
        }
        n += result.n;
    }
}
