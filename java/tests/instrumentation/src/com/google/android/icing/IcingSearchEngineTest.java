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

package com.google.android.icing;

import static com.google.common.truth.Truth.assertThat;

import android.content.Context;

import androidx.test.InstrumentationRegistry;

import com.google.android.icing.proto.DeleteByNamespaceResultProto;
import com.google.android.icing.proto.DeleteBySchemaTypeResultProto;
import com.google.android.icing.proto.DeleteResultProto;
import com.google.android.icing.proto.DocumentProto;
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
import com.google.android.icing.proto.ResultSpecProto;
import com.google.android.icing.proto.SchemaProto;
import com.google.android.icing.proto.SchemaTypeConfigProto;
import com.google.android.icing.proto.ScoringSpecProto;
import com.google.android.icing.proto.SearchResultProto;
import com.google.android.icing.proto.SearchSpecProto;
import com.google.android.icing.proto.SetSchemaResultProto;
import com.google.android.icing.proto.StatusProto;
import com.google.android.icing.proto.TermMatchType;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This test is not intended to fully test the functionality of each API. But rather to test the JNI
 * wrapper of Icing library.
 */
@RunWith(JUnit4.class)
public final class IcingSearchEngineTest {

  private static final String EMAIL_TYPE = "Email";

  private Context mContext;
  private String mFilesDir;

  static SchemaTypeConfigProto createEmailTypeConfig() {
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

  static DocumentProto createEmailDocument(String namespace, String uri) {
    return DocumentProto.newBuilder()
        .setNamespace(namespace)
        .setUri(uri)
        .setSchema(EMAIL_TYPE)
        .setCreationTimestampMs(1) // Arbitrary non-zero number so Icing doesn't override it
        .build();
  }

  @Before
  public void setUp() throws Exception {
    mContext = InstrumentationRegistry.getInstrumentation().getContext();
    mFilesDir = mContext.getFilesDir().getCanonicalPath();
  }

  @Test
  public void testInitialize() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);

    InitializeResultProto initializeResultProto = icing.initialize();
    assertThat(initializeResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
  }

  @Test
  public void testSetAndGetSchema() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

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
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icing.setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);

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
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icing.setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);

    DocumentProto emailDocument =
        createEmailDocument("namespace", "uri").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
            .build();
    PutResultProto putResultProto = icing.put(emailDocument);

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
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icing.setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    PutResultProto putResultProto = icing.put(emailDocument);

    DeleteResultProto deleteResultProto = icing.delete("namespace", "uri");
    assertThat(deleteResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteByNamespace() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icing.setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    PutResultProto putResultProto = icing.put(emailDocument);

    DeleteByNamespaceResultProto deleteByNamespaceResultProto =
        icing.deleteByNamespace("namespace");
    assertThat(deleteByNamespaceResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteBySchemaType() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icing.setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    PutResultProto putResultProto = icing.put(emailDocument);

    DeleteBySchemaTypeResultProto deleteBySchemaTypeResultProto =
        icing.deleteBySchemaType(EMAIL_TYPE);
    assertThat(deleteBySchemaTypeResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testPersistToDisk() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

    PersistToDiskResultProto persistToDiskResultProto = icing.persistToDisk();
    assertThat(persistToDiskResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
  }

  @Test
  public void testOptimize() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(mFilesDir).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    InitializeResultProto initializeResultProto = icing.initialize();

    OptimizeResultProto optimizeResultProto = icing.optimize();
    assertThat(optimizeResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.OK);
  }
}
