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
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.android.icing.proto.DeleteByNamespaceResultProto;
import com.google.android.icing.proto.DeleteBySchemaTypeResultProto;
import com.google.android.icing.proto.DeleteResultProto;
import com.google.android.icing.proto.DocumentProto;
import com.google.android.icing.proto.GetAllNamespacesResultProto;
import com.google.android.icing.proto.GetOptimizeInfoResultProto;
import com.google.android.icing.proto.GetResultProto;
import com.google.android.icing.proto.GetSchemaResultProto;
import com.google.android.icing.proto.GetSchemaTypeResultProto;
import com.google.android.icing.proto.IcingSearchEngineOptions;
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
import com.google.android.icing.proto.StringIndexingConfig;
import com.google.android.icing.proto.StringIndexingConfig.TokenizerType;
import com.google.android.icing.proto.TermMatchType;
import com.google.android.icing.IcingSearchEngine;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This test is not intended to fully test the functionality of each API. But rather to test the JNI
 * wrapper and Java interfaces of Icing library {@link IcingSearchEngine}.
 */
@RunWith(JUnit4.class)
public final class IcingSearchEngineTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String EMAIL_TYPE = "Email";

  private File tempDir;

  private static SchemaTypeConfigProto createEmailTypeConfig() {
    return SchemaTypeConfigProto.newBuilder()
        .setSchemaType(EMAIL_TYPE)
        .addProperties(
            PropertyConfigProto.newBuilder()
                .setPropertyName("subject")
                .setDataType(PropertyConfigProto.DataType.Code.STRING)
                .setCardinality(PropertyConfigProto.Cardinality.Code.OPTIONAL)
                .setStringIndexingConfig(
                    StringIndexingConfig.newBuilder()
                        .setTokenizerType(TokenizerType.Code.PLAIN)
                        .setTermMatchType(TermMatchType.Code.PREFIX)))
        .addProperties(
            PropertyConfigProto.newBuilder()
                .setPropertyName("body")
                .setDataType(PropertyConfigProto.DataType.Code.STRING)
                .setCardinality(PropertyConfigProto.Cardinality.Code.OPTIONAL)
                .setStringIndexingConfig(
                    StringIndexingConfig.newBuilder()
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
    tempDir = temporaryFolder.newFolder();
  }

  @Test
  public void testInitialize() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);

    InitializeResultProto initializeResultProto = icing.initialize();
    assertStatusOk(initializeResultProto.getStatus());
  }

  @Test
  public void testSetAndGetSchema() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icing.setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);
    assertStatusOk(setSchemaResultProto.getStatus());

    GetSchemaResultProto getSchemaResultProto = icing.getSchema();
    assertStatusOk(getSchemaResultProto.getStatus());
    assertThat(getSchemaResultProto.getSchema()).isEqualTo(schema);

    GetSchemaTypeResultProto getSchemaTypeResultProto =
        icing.getSchemaType(emailTypeConfig.getSchemaType());
    assertStatusOk(getSchemaTypeResultProto.getStatus());
    assertThat(getSchemaTypeResultProto.getSchemaTypeConfig()).isEqualTo(emailTypeConfig);
  }

  @Test
  public void testPutAndGetDocuments() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

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
    assertStatusOk(putResultProto.getStatus());

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertStatusOk(getResultProto.getStatus());
    assertThat(getResultProto.getDocument()).isEqualTo(emailDocument);
  }

  @Test
  public void testSearch() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

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
    assertStatusOk(icing.put(emailDocument).getStatus());

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
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
    assertThat(searchResultProto.getResults(0).getDocument()).isEqualTo(emailDocument);
  }

  @Test
  public void testGetNextPage() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    Map<String, DocumentProto> documents = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      DocumentProto emailDocument =
          createEmailDocument("namespace", "uri:" + i).toBuilder()
              .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
              .build();
      documents.put("uri:" + i, emailDocument);
      assertWithMessage(icing.put(emailDocument).getStatus().getMessage())
          .that(icing.put(emailDocument).getStatus().getCode())
          .isEqualTo(StatusProto.Code.OK);
    }

    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("foo")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();
    ResultSpecProto resultSpecProto = ResultSpecProto.newBuilder().setNumPerPage(1).build();

    SearchResultProto searchResultProto =
        icing.search(searchSpec, ScoringSpecProto.getDefaultInstance(), resultSpecProto);
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
    DocumentProto resultDocument = searchResultProto.getResults(0).getDocument();
    assertThat(resultDocument).isEqualTo(documents.remove(resultDocument.getUri()));

    // fetch rest pages
    for (int i = 1; i < 5; i++) {
      searchResultProto = icing.getNextPage(searchResultProto.getNextPageToken());
      assertWithMessage(searchResultProto.getStatus().getMessage())
          .that(searchResultProto.getStatus().getCode())
          .isEqualTo(StatusProto.Code.OK);
      assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
      resultDocument = searchResultProto.getResults(0).getDocument();
      assertThat(resultDocument).isEqualTo(documents.remove(resultDocument.getUri()));
    }

    // invalidate rest result
    icing.invalidateNextPageToken(searchResultProto.getNextPageToken());

    searchResultProto = icing.getNextPage(searchResultProto.getNextPageToken());
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(0);
  }

  @Test
  public void testDelete() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icing.put(emailDocument).getStatus());

    DeleteResultProto deleteResultProto = icing.delete("namespace", "uri");
    assertStatusOk(deleteResultProto.getStatus());

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteByNamespace() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icing.put(emailDocument).getStatus());

    DeleteByNamespaceResultProto deleteByNamespaceResultProto =
        icing.deleteByNamespace("namespace");
    assertStatusOk(deleteByNamespaceResultProto.getStatus());

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteBySchemaType() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icing
                .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icing.put(emailDocument).getStatus());

    DeleteBySchemaTypeResultProto deleteBySchemaTypeResultProto =
        icing.deleteBySchemaType(EMAIL_TYPE);
    assertStatusOk(deleteBySchemaTypeResultProto.getStatus());

    GetResultProto getResultProto = icing.get("namespace", "uri");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }


  @Test
  public void testDeleteByQuery() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
        icing
            .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
            .getStatus()
            .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument1 =
        createEmailDocument("namespace", "uri1").toBuilder()
        .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
        .build();;
    assertStatusOk(icing.put(emailDocument1).getStatus());
    DocumentProto emailDocument2 =
        createEmailDocument("namespace", "uri2").toBuilder()
        .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("bar"))
        .build();;
    assertStatusOk(icing.put(emailDocument2).getStatus());

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
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
    assertThat(searchResultProto.getResults(0).getDocument()).isEqualTo(emailDocument1);

    DeleteResultProto deleteResultProto = icing.deleteByQuery(searchSpec);
    assertStatusOk(deleteResultProto.getStatus());

    GetResultProto getResultProto = icing.get("namespace", "uri1");
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
    getResultProto = icing.get("namespace", "uri2");
    assertStatusOk(getResultProto.getStatus());
  }

  @Test
  public void testPersistToDisk() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    PersistToDiskResultProto persistToDiskResultProto = icing.persistToDisk();
    assertStatusOk(persistToDiskResultProto.getStatus());
  }

  @Test
  public void testOptimize() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    OptimizeResultProto optimizeResultProto = icing.optimize();
    assertStatusOk(optimizeResultProto.getStatus());
  }

  @Test
  public void testGetOptimizeInfo() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    GetOptimizeInfoResultProto getOptimizeInfoResultProto = icing.getOptimizeInfo();
    assertStatusOk(getOptimizeInfoResultProto.getStatus());
    assertThat(getOptimizeInfoResultProto.getOptimizableDocs()).isEqualTo(0);
    assertThat(getOptimizeInfoResultProto.getEstimatedOptimizableBytes()).isEqualTo(0);
  }

  @Test
  public void testGetAllNamespaces() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
        icing
            .setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false)
            .getStatus()
            .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icing.put(emailDocument).getStatus());

    GetAllNamespacesResultProto getAllNamespacesResultProto = icing.getAllNamespaces();
    assertStatusOk(getAllNamespacesResultProto.getStatus());
    assertThat(getAllNamespacesResultProto.getNamespacesList()).containsExactly("namespace");
  }

  @Test
  public void testReset() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    ResetResultProto resetResultProto = icing.reset();
    assertStatusOk(resetResultProto.getStatus());
  }

  private static void assertStatusOk(StatusProto status) {
    assertWithMessage(status.getMessage()).that(status.getCode()).isEqualTo(StatusProto.Code.OK);
  }
}
