// Copyright (C) 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.android.icing;

import static com.google.common.truth.Truth.assertThat;

import androidx.test.core.app.ApplicationProvider;
import com.google.android.icing.proto.DeleteByNamespaceResultProto;
import com.google.android.icing.proto.DeleteBySchemaTypeResultProto;
import com.google.android.icing.proto.DeleteResultProto;
import com.google.android.icing.proto.DocumentProto;
import com.google.android.icing.proto.GetOptimizeInfoResultProto;
import com.google.android.icing.proto.GetResultProto;
import com.google.android.icing.proto.GetSchemaResultProto;
import com.google.android.icing.proto.GetSchemaTypeResultProto;
import com.google.android.icing.proto.IcingSearchEngineOptions;
import com.google.android.icing.proto.IndexingConfig;
import com.google.android.icing.proto.IndexingConfig.TokenizerType;
import com.google.android.icing.proto.InitializeResultProto;
import com.google.android.icing.proto.OptimizeResultProto;
import com.google.android.icing.proto.PersistToDiskResultProto;
import com.google.android.icing.proto.PropertyConfigProto;
import com.google.android.icing.proto.PropertyProto;
import com.google.android.icing.proto.PutResultProto;
import com.google.android.icing.proto.ResetResultProto;
import com.google.android.icing.proto.ResultSpecProto;
import com.google.android.icing.proto.SchemaProto;
import com.google.android.icing.proto.SchemaTypeConfigProto;
import com.google.android.icing.proto.ScoringSpecProto;
import com.google.android.icing.proto.SearchResultProto;
import com.google.android.icing.proto.SearchSpecProto;
import com.google.android.icing.proto.SetSchemaResultProto;
import com.google.android.icing.proto.StatusProto;
import com.google.android.icing.proto.TermMatchType;
import com.google.android.libraries.mdi.search.IcingSearchEngine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;

/**
 * This test is not intended to fully test the functionality of each API. But rather to test the JNI
 * wrapper and Java interfaces of Icing library {@link IcingSearchEngine}.
 */
@RunWith(RobolectricTestRunner.class)
public final class IcingSearchEngineTest {

  private static final String EMAIL_TYPE = "Email";

  private String filesDir;

  private static SchemaTypeConfigProto createEmailTypeConfig() {
    return SchemaTypeConfigProto.newBuilder()
        .setSchemaType(EMAIL_TYPE)
        .addProperties(
            PropertyConfigProto.newBuilder()
                .setPropertyName("subject")
                .setDataType(PropertyConfigProto.DataType.Code.STRING)
                .setCardinality(PropertyConfigProto.Cardinality.Code.OPTIONAL)
                .setIndexingConfig(
                    IndexingConfig.newBuilder()
                        .setTokenizerType(TokenizerType.Code.PLAIN)
                        .setTermMatchType(TermMatchType.Code.PREFIX)))
        .addProperties(
            PropertyConfigProto.newBuilder()
                .setPropertyName("body")
                .setDataType(PropertyConfigProto.DataType.Code.STRING)
                .setCardinality(PropertyConfigProto.Cardinality.Code.OPTIONAL)
                .setIndexingConfig(
                    IndexingConfig.newBuilder()
                        .setTokenizerType(TokenizerType.Code.PLAIN)
                        .setTermMatchType(TermMatchType.Code.PREFIX)))
        .build();
  }

  private static DocumentProto createEmailDocument(String namespace, String uri) {
    return DocumentProto.newBuilder()
        .setNamespace(namespace)
        .setUri(uri)
        .setSchema(EMAIL_TYPE)
        .setCreationTimestampMs(1) // Arbitrary non-zero number so Icing doesn't override it
        .build();
  }

  @Before
  public void setUp() throws Exception {
    filesDir = ApplicationProvider.getApplicationContext().getFilesDir().getCanonicalPath();
  }

  @Test
  public void testInitialize() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);

    InitializeResultProto initializeResultProto = icing.initialize();
    assertThat(initializeResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
  }

  @Test
  public void testSetAndGetSchema() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icing.setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);
    assertThat(setSchemaResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetSchemaResultProto getSchemaResultProto = icing.getSchema();
    assertThat(getSchemaResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
    assertThat(getSchemaResultProto.getSchema()).isEqualTo(schema);

    GetSchemaTypeResultProto getSchemaTypeResultProto =
        icing.getSchemaType(emailTypeConfig.getSchemaType());
    assertThat(getSchemaTypeResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
    assertThat(getSchemaTypeResultProto.getSchemaTypeConfig()).isEqualTo(emailTypeConfig);
  }

  @Test
  public void testPutAndGetDocuments() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    PutResultProto putResultProto = icing.put(emailDocument);
    assertThat(putResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
    assertThat(getResultProto.getDocument()).isEqualTo(emailDocument);
  }

  @Test
  public void testSearch() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument =
        createEmailDocument("namespace", "uri").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
            .build();
    assertThat(icing.put(emailDocument).getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("foo")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();

    SearchResultProto searchResultProto =
        icing.search(
            searchSpec,
            ScoringSpecProto.getDefaultInstance(),
            ResultSpecProto.getDefaultInstance());
    assertThat(searchResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
    assertThat(searchResultProto.getResults(0).getDocument()).isEqualTo(emailDocument);
  }

  @Test
  public void testDelete() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertThat(icing.put(emailDocument).getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    DeleteResultProto deleteResultProto = icing.delete("namespace", "uri");
    assertThat(deleteResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteByNamespace() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertThat(icing.put(emailDocument).getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    DeleteByNamespaceResultProto deleteByNamespaceResultProto =
        icing.deleteByNamespace("namespace");
    assertThat(deleteByNamespaceResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteBySchemaType() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertThat(icing.put(emailDocument).getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    DeleteBySchemaTypeResultProto deleteBySchemaTypeResultProto =
        icing.deleteBySchemaType(EMAIL_TYPE);
    assertThat(deleteBySchemaTypeResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testPersistToDisk() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    PersistToDiskResultProto persistToDiskResultProto = icing.persistToDisk();
    assertThat(persistToDiskResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
  }

  @Test
  public void testOptimize() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    OptimizeResultProto optimizeResultProto = icing.optimize();
    assertThat(optimizeResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
  }

  @Test
  public void testGetOptimizeInfo() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetOptimizeInfoResultProto getOptimizeInfoResultProto = icing.getOptimizeInfo();
    assertThat(getOptimizeInfoResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
    assertThat(getOptimizeInfoResultProto.getOptimizableDocs()).isEqualTo(0);
    assertThat(getOptimizeInfoResultProto.getEstimatedOptimizableBytes()).isEqualTo(0);
  }

  @Test
  public void testReset() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(filesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertThat(icing.initialize().getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    ResetResultProto resetResultProto = icing.reset();
    assertThat(resetResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
  }
}
