/*
 * Copyright (C) 2020 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package androidx.appsearch.smoketest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import androidx.appsearch.app.AppSearchManager;
import androidx.appsearch.app.AppSearchManager.PutDocumentsRequest;
import androidx.appsearch.app.AppSearchManager.SetSchemaRequest;
import androidx.appsearch.app.AppSearchSchema;
import androidx.appsearch.app.AppSearchSchema.PropertyConfig;
import androidx.appsearch.app.SearchResults;
import androidx.appsearch.app.SearchSpec;
import androidx.test.core.app.ApplicationProvider;
import androidx.test.filters.SmallTest;

import com.google.android.icing.proto.IcingSearchEngineOptions;

import org.junit.Test;

@SmallTest
public class AppSearchSmokeTest {
    @Test
    public void smokeTest() throws Exception {
        IcingSearchEngineOptions o = IcingSearchEngineOptions.newBuilder().build();
        AppSearchManager appSearch =
                new AppSearchManager.Builder(ApplicationProvider.getApplicationContext())
                        .build()
                        .get()
                        .getResultValue();
        AppSearchSchema schema =
                new AppSearchSchema.Builder("testType")
                        .addProperty(
                                new PropertyConfig.Builder("prop")
                                        .setDataType(PropertyConfig.DATA_TYPE_STRING)
                                        .setCardinality(PropertyConfig.CARDINALITY_OPTIONAL)
                                        .setIndexingType(PropertyConfig.INDEXING_TYPE_PREFIXES)
                                        .setTokenizerType(PropertyConfig.TOKENIZER_TYPE_PLAIN)
                                        .build())
                        .build();
        appSearch
                .setSchema(new SetSchemaRequest.Builder().addSchema(schema).build())
                .get()
                .getResultValue();
    }

    @Test
    public void smokeTestAnnotationProcessor() throws Exception {
        AppSearchManager appSearch =
                new AppSearchManager.Builder(ApplicationProvider.getApplicationContext())
                        .build()
                        .get()
                        .getResultValue();
        appSearch
                .setSchema(new SetSchemaRequest.Builder().addDataClass(TestDataClass.class).build())
                .get()
                .getResultValue();

        TestDataClass input = new TestDataClass("uri1", "avocado");
        appSearch
                .putDocuments(new PutDocumentsRequest.Builder().addDataClass(input).build())
                .get()
                .checkSuccess();
        SearchResults results =
                appSearch
                        .query(
                                "av",
                                SearchSpec.newBuilder().setTermMatchType(
                                        SearchSpec.TERM_MATCH_TYPE_PREFIX).build())
                        .get()
                        .getResultValue();

        assertTrue(results.hasNext());
        SearchResults.Result result = results.next();
        assertFalse(results.hasNext());

        assertEquals("uri1", result.getDocument().getUri());
        assertEquals("avocado", result.getDocument().getPropertyString("body"));
        TestDataClass output = result.getDocument().toDataClass(TestDataClass.class);
        assertEquals("uri1", output.getUri());
        assertEquals("avocado", output.getBody());
    }
}
