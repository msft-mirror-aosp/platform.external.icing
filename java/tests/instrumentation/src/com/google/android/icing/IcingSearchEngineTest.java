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

import com.google.android.icing.IcingSearchEngine;
import com.google.android.icing.proto.BatchGetResultProto;
import com.google.android.icing.proto.BatchPutResultProto;
import com.google.android.icing.proto.BlobProto;
import com.google.android.icing.proto.DebugInfoResultProto;
import com.google.android.icing.proto.DebugInfoVerbosity;
import com.google.android.icing.proto.DeleteByNamespaceResultProto;
import com.google.android.icing.proto.DeleteByQueryResultProto;
import com.google.android.icing.proto.DeleteBySchemaTypeResultProto;
import com.google.android.icing.proto.DeleteResultProto;
import com.google.android.icing.proto.DocumentProto;
import com.google.android.icing.proto.GetAllNamespacesResultProto;
import com.google.android.icing.proto.GetOptimizeInfoResultProto;
import com.google.android.icing.proto.GetResultProto;
import com.google.android.icing.proto.GetResultSpecProto;
import com.google.android.icing.proto.GetSchemaResultProto;
import com.google.android.icing.proto.GetSchemaTypeResultProto;
import com.google.android.icing.proto.IcingSearchEngineOptions;
import com.google.android.icing.proto.InitializeResultProto;
import com.google.android.icing.proto.LogSeverity;
import com.google.android.icing.proto.OptimizeResultProto;
import com.google.android.icing.proto.PersistToDiskResultProto;
import com.google.android.icing.proto.PersistType;
import com.google.android.icing.proto.PropertyConfigProto;
import com.google.android.icing.proto.PropertyProto;
import com.google.android.icing.proto.PutDocumentRequest;
import com.google.android.icing.proto.PutResultProto;
import com.google.android.icing.proto.ReportUsageResultProto;
import com.google.android.icing.proto.ResetResultProto;
import com.google.android.icing.proto.ResultSpecProto;
import com.google.android.icing.proto.SchemaProto;
import com.google.android.icing.proto.SchemaTypeConfigProto;
import com.google.android.icing.proto.ScoringSpecProto;
import com.google.android.icing.proto.SearchResultProto;
import com.google.android.icing.proto.SearchSpecProto;
import com.google.android.icing.proto.SetSchemaRequestProto;
import com.google.android.icing.proto.SetSchemaResultProto;
import com.google.android.icing.proto.SnippetMatchProto;
import com.google.android.icing.proto.SnippetProto;
import com.google.android.icing.proto.StatusProto;
import com.google.android.icing.proto.StorageInfoResultProto;
import com.google.android.icing.proto.StringIndexingConfig;
import com.google.android.icing.proto.StringIndexingConfig.TokenizerType;
import com.google.android.icing.proto.SuggestionResponse;
import com.google.android.icing.proto.SuggestionScoringSpecProto;
import com.google.android.icing.proto.SuggestionSpecProto;
import com.google.android.icing.proto.TermMatchType;
import com.google.android.icing.proto.TermMatchType.Code;
import com.google.android.icing.proto.UsageReport;
import com.google.android.icing.protobuf.ByteString;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

  private static final String SCHEMA_DATABASE_DELIMITER = "/";
  private static final String EMAIL_TYPE = "Email";

  private File tempDir;

  private IcingSearchEngine icingSearchEngine;

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

  private static SchemaTypeConfigProto createEmailTypeConfigWithDatabase(String database) {
    return SchemaTypeConfigProto.newBuilder()
        .setSchemaType(database + SCHEMA_DATABASE_DELIMITER + EMAIL_TYPE)
        .setDatabase(database)
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

  /** Generate an array contains random bytes for the given length. */
  private static byte[] generateRandomBytes(int length) {
    byte[] bytes = new byte[length];
    Random rd = new Random(); // creating Random object
    rd.nextBytes(bytes);
    return bytes;
  }

  /** Calculate the sha-256 digest for the given data. */
  private static byte[] calculateDigest(byte[] data) throws NoSuchAlgorithmException {
    MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
    messageDigest.update(data);
    return messageDigest.digest();
  }

  @Before
  public void setUp() throws Exception {
    tempDir = temporaryFolder.newFolder();
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder().setBaseDir(tempDir.getCanonicalPath()).build();
    icingSearchEngine = new IcingSearchEngine(options);
  }

  @After
  public void tearDown() throws Exception {
    icingSearchEngine.close();
  }

  @Test
  public void testInitialize() throws Exception {
    InitializeResultProto initializeResultProto = icingSearchEngine.initialize();
    assertStatusOk(initializeResultProto.getStatus());
  }

  @Test
  public void testSetAndGetSchema() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    SetSchemaResultProto setSchemaResultProto =
        icingSearchEngine.setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false);
    assertStatusOk(setSchemaResultProto.getStatus());

    GetSchemaResultProto getSchemaResultProto = icingSearchEngine.getSchema();
    assertStatusOk(getSchemaResultProto.getStatus());
    assertThat(getSchemaResultProto.getSchema()).isEqualTo(schema);

    GetSchemaTypeResultProto getSchemaTypeResultProto =
        icingSearchEngine.getSchemaType(emailTypeConfig.getSchemaType());
    assertStatusOk(getSchemaTypeResultProto.getStatus());
    assertThat(getSchemaTypeResultProto.getSchemaTypeConfig()).isEqualTo(emailTypeConfig);
  }

  // TODO: b/383379132 - Re-enable this test once the JNI API is pre-registered and dropped back
  // into g3.
  @Ignore
  @Test
  public void setAndGetSchemaWithDatabase_ok() throws Exception {
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder()
            .setBaseDir(tempDir.getCanonicalPath())
            .setEnableSchemaDatabase(true)
            .build();
    IcingSearchEngine icingSearchEngine = new IcingSearchEngine(options);
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    String db1 = "db1";
    String db2 = "db2";
    SchemaProto db1Schema =
        SchemaProto.newBuilder().addTypes(createEmailTypeConfigWithDatabase(db1)).build();
    SchemaProto db2Schema =
        SchemaProto.newBuilder().addTypes(createEmailTypeConfigWithDatabase(db2)).build();

    SetSchemaRequestProto requestProto1 =
        SetSchemaRequestProto.newBuilder()
            .setSchema(db1Schema)
            .setDatabase(db1)
            .setIgnoreErrorsAndDeleteDocuments(false)
            .build();
    SetSchemaResultProto setSchemaResultProto =
        icingSearchEngine.setSchemaWithRequestProto(requestProto1);
    assertStatusOk(setSchemaResultProto.getStatus());

    SetSchemaRequestProto requestProto2 =
        SetSchemaRequestProto.newBuilder()
            .setSchema(db2Schema)
            .setDatabase(db2)
            .setIgnoreErrorsAndDeleteDocuments(false)
            .build();
    setSchemaResultProto = icingSearchEngine.setSchemaWithRequestProto(requestProto2);
    assertStatusOk(setSchemaResultProto.getStatus());

    // Get schema for individual databases.
    GetSchemaResultProto getSchemaResultProto = icingSearchEngine.getSchemaForDatabase(db1);
    assertStatusOk(getSchemaResultProto.getStatus());
    assertThat(getSchemaResultProto.getSchema()).isEqualTo(db1Schema);

    getSchemaResultProto = icingSearchEngine.getSchemaForDatabase(db2);
    assertStatusOk(getSchemaResultProto.getStatus());
    assertThat(getSchemaResultProto.getSchema()).isEqualTo(db2Schema);

    // The getSchema() API should still return the full schema.
    SchemaProto fullSchema =
        SchemaProto.newBuilder()
            .addTypes(createEmailTypeConfigWithDatabase(db1))
            .addTypes(createEmailTypeConfigWithDatabase(db2))
            .build();
    getSchemaResultProto = icingSearchEngine.getSchema();
    assertStatusOk(getSchemaResultProto.getStatus());
    assertThat(getSchemaResultProto.getSchema()).isEqualTo(fullSchema);
  }

  @Test
  public void testPutAndGetDocuments() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    PutResultProto putResultProto = icingSearchEngine.put(emailDocument);
    assertStatusOk(putResultProto.getStatus());

    GetResultProto getResultProto =
        icingSearchEngine.get("namespace", "uri", GetResultSpecProto.getDefaultInstance());
    assertStatusOk(getResultProto.getStatus());
    assertThat(getResultProto.getDocument()).isEqualTo(emailDocument);
  }

  @Test
  public void testBatchPutAndGetDocuments() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument1 = createEmailDocument("namespace", "uri1");
    DocumentProto emailDocument2 = createEmailDocument("namespace", "uri2");
    PutDocumentRequest putDocumentRequest =
        PutDocumentRequest.newBuilder()
            .addDocuments(emailDocument1)
            .addDocuments(emailDocument2)
            .build();
    BatchPutResultProto batchPutResultProto = icingSearchEngine.batchPut(putDocumentRequest);

    assertStatusOk(batchPutResultProto.getStatus());
    assertThat(batchPutResultProto.getPutResultProtos(0).getUri()).isEqualTo("uri1");
    assertStatusOk(batchPutResultProto.getPutResultProtos(0).getStatus());
    assertThat(batchPutResultProto.getPutResultProtos(1).getUri()).isEqualTo("uri2");
    assertStatusOk(batchPutResultProto.getPutResultProtos(1).getStatus());

    // PersistToDiskResultProto should not be set if persist_type is not set in the
    // PutDocumentRequest.
    assertThat(batchPutResultProto.getPersistToDiskResultProto().getStatus().getCode())
        .isEqualTo(StatusProto.Code.UNKNOWN);

    GetResultSpecProto getResultSpecProto =
        GetResultSpecProto.newBuilder()
            .setNamespaceRequested("namespace")
            .addIds("uri1")
            .addIds("uri2")
            .build();
    BatchGetResultProto batchGetResultProto = icingSearchEngine.batchGet(getResultSpecProto);

    assertStatusOk(batchGetResultProto.getStatus());
    // Check doc1
    DocumentProto document = batchGetResultProto.getGetResultProtos(0).getDocument();
    assertStatusOk(batchGetResultProto.getGetResultProtos(0).getStatus());
    assertThat(document).isEqualTo(emailDocument1);
    // Check doc2
    document = batchGetResultProto.getGetResultProtos(1).getDocument();
    assertStatusOk(batchGetResultProto.getGetResultProtos(1).getStatus());
    assertThat(document).isEqualTo(emailDocument2);
  }

  @Test
  public void testBatchGetWithEmptyResult() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument1 = createEmailDocument("namespace", "uri1");
    DocumentProto emailDocument2 = createEmailDocument("namespace", "uri2");
    PutDocumentRequest putDocumentRequest =
        PutDocumentRequest.newBuilder()
            .addDocuments(emailDocument1)
            .addDocuments(emailDocument2)
            .build();
    BatchPutResultProto batchPutResultProto = icingSearchEngine.batchPut(putDocumentRequest);
    assertStatusOk(batchPutResultProto.getStatus());

    // no ids.
    GetResultSpecProto getResultSpecProto =
        GetResultSpecProto.newBuilder().setNamespaceRequested("namespace").build();
    BatchGetResultProto batchGetResultProto = icingSearchEngine.batchGet(getResultSpecProto);

    // Check no doc returned if no ids are specified.
    assertStatusOk(batchGetResultProto.getStatus());
    assertThat(batchGetResultProto.getGetResultProtosList()).isEmpty();

    // empty namespace.
    getResultSpecProto = GetResultSpecProto.newBuilder().addIds("uri1").build();
    batchGetResultProto = icingSearchEngine.batchGet(getResultSpecProto);
    assertStatusOk(batchGetResultProto.getStatus());
    assertThat(batchGetResultProto.getGetResultProtosList()).hasSize(1);
    assertThat(batchGetResultProto.getGetResultProtos(0).getStatus().getCode())
        .isEqualTo(StatusProto.Code.NOT_FOUND);

    // different namespace.
    getResultSpecProto =
        GetResultSpecProto.newBuilder()
            .setNamespaceRequested("otherNameSpace")
            .addIds("uri1")
            .addIds("uri2")
            .build();
    batchGetResultProto = icingSearchEngine.batchGet(getResultSpecProto);

    // Check not found returned if namespace is different.
    assertStatusOk(batchGetResultProto.getStatus());
    assertThat(batchGetResultProto.getGetResultProtosList()).hasSize(2);
    assertThat(batchGetResultProto.getGetResultProtos(0).getStatus().getCode())
        .isEqualTo(StatusProto.Code.NOT_FOUND);
    assertThat(batchGetResultProto.getGetResultProtos(1).getStatus().getCode())
        .isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testBatchPutWithDuplicatedDocuments() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    // Two docs with same uri.
    DocumentProto emailDocument1 = createEmailDocument("namespace", "uri");
    DocumentProto emailDocument2 = createEmailDocument("namespace", "uri");
    PutDocumentRequest putDocumentRequest =
        PutDocumentRequest.newBuilder()
            .addDocuments(emailDocument1)
            .addDocuments(emailDocument2)
            .build();
    BatchPutResultProto batchPutResultProto = icingSearchEngine.batchPut(putDocumentRequest);

    // We should still get two putResults back. That's intended behavior.
    assertThat(batchPutResultProto.getPutResultProtosList()).hasSize(2);
    assertThat(batchPutResultProto.getPutResultProtos(0).getUri()).isEqualTo("uri");
    assertStatusOk(batchPutResultProto.getPutResultProtos(0).getStatus());
    assertThat(batchPutResultProto.getPutResultProtos(0).getWasReplacement()).isFalse();
    assertThat(batchPutResultProto.getPutResultProtos(1).getUri()).isEqualTo("uri");
    assertStatusOk(batchPutResultProto.getPutResultProtos(1).getStatus());
    assertThat(batchPutResultProto.getPutResultProtos(1).getWasReplacement()).isTrue();

    // PersistToDiskResultProto should not be set if persist_type is not set in the
    // PutDocumentRequest.
    assertThat(batchPutResultProto.getPersistToDiskResultProto().getStatus().getCode())
        .isEqualTo(StatusProto.Code.UNKNOWN);
  }

  @Test
  public void testBatchPutWithEmptyRequest() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    PutDocumentRequest putDocumentRequest = PutDocumentRequest.getDefaultInstance();
    BatchPutResultProto batchPutResultProto = icingSearchEngine.batchPut(putDocumentRequest);

    BatchPutResultProto expected =
        BatchPutResultProto.newBuilder()
            .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.OK))
            .build();
    assertThat(batchPutResultProto).isEqualTo(expected);

    // PersistToDiskResultProto should not be set if persist_type is not set in the
    // PutDocumentRequest.
    assertThat(batchPutResultProto.getPersistToDiskResultProto().getStatus().getCode())
        .isEqualTo(StatusProto.Code.UNKNOWN);
  }

  @Test
  public void testBatchPutAndGetDocumentsWithError() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);
    // Document 1 has no namespace.
    DocumentProto emailDocument1 = DocumentProto.newBuilder().setUri("uri1").build();
    DocumentProto emailDocument2 = createEmailDocument("namespace", "uri2");
    PutDocumentRequest putDocumentRequest =
        PutDocumentRequest.newBuilder()
            .addDocuments(emailDocument1)
            .addDocuments(emailDocument2)
            .build();
    BatchPutResultProto batchPutResultProto = icingSearchEngine.batchPut(putDocumentRequest);

    PutResultProto putResult1 = batchPutResultProto.getPutResultProtos(0);
    // result0 error as namespace is missing.
    assertThat(putResult1.getUri()).isEqualTo("uri1");
    assertWithMessage(putResult1.getStatus().getMessage())
        .that(putResult1.getStatus().getCode())
        .isEqualTo(StatusProto.Code.INVALID_ARGUMENT);
    // result1 is ok.
    assertThat(batchPutResultProto.getPutResultProtos(1).getUri()).isEqualTo("uri2");
    assertStatusOk(batchPutResultProto.getPutResultProtos(1).getStatus());

    // PersistToDiskResultProto should not be set if persist_type is not set in the
    // PutDocumentRequest.
    assertThat(batchPutResultProto.getPersistToDiskResultProto().getStatus().getCode())
        .isEqualTo(StatusProto.Code.UNKNOWN);

    // Check document 1
    GetResultProto getResultProto =
        icingSearchEngine.get("namespace", "uri1", GetResultSpecProto.getDefaultInstance());
    assertWithMessage(getResultProto.getStatus().getMessage())
        .that(getResultProto.getStatus().getCode())
        .isEqualTo(StatusProto.Code.NOT_FOUND);
    // check document 2
    getResultProto =
        icingSearchEngine.get("namespace", "uri2", GetResultSpecProto.getDefaultInstance());
    assertStatusOk(getResultProto.getStatus());
    assertThat(getResultProto.getDocument()).isEqualTo(emailDocument2);
  }

  @Test
  public void testBatchPutWithPersistToDisk() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument1 = createEmailDocument("namespace", "uri1");
    DocumentProto emailDocument2 = createEmailDocument("namespace", "uri2");
    PutDocumentRequest putDocumentRequest =
        PutDocumentRequest.newBuilder()
            .addDocuments(emailDocument1)
            .addDocuments(emailDocument2)
            .setPersistType(PersistType.Code.FULL)
            .build();
    BatchPutResultProto batchPutResultProto = icingSearchEngine.batchPut(putDocumentRequest);

    assertThat(batchPutResultProto.getPutResultProtos(0).getUri()).isEqualTo("uri1");
    assertStatusOk(batchPutResultProto.getPutResultProtos(0).getStatus());
    assertThat(batchPutResultProto.getPutResultProtos(1).getUri()).isEqualTo("uri2");
    assertStatusOk(batchPutResultProto.getPutResultProtos(1).getStatus());

    // PersistToDisk should be called if persist_type is set in the PutDocumentRequest.
    assertStatusOk(batchPutResultProto.getPersistToDiskResultProto().getStatus());
  }

  @Test
  public void testSearch() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument =
        createEmailDocument("namespace", "uri").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
            .build();
    assertStatusOk(icingSearchEngine.put(emailDocument).getStatus());

    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("foo")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();

    SearchResultProto searchResultProto =
        icingSearchEngine.search(
            searchSpec,
            ScoringSpecProto.getDefaultInstance(),
            ResultSpecProto.getDefaultInstance());
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
    assertThat(searchResultProto.getResults(0).getDocument()).isEqualTo(emailDocument);

    assertThat(searchResultProto.getQueryStats().hasNativeToJavaStartTimestampMs()).isTrue();
    assertThat(searchResultProto.getQueryStats().hasNativeToJavaJniLatencyMs()).isTrue();
    assertThat(searchResultProto.getQueryStats().hasJavaToNativeJniLatencyMs()).isTrue();
    assertThat(searchResultProto.getQueryStats().getNativeToJavaStartTimestampMs())
        .isGreaterThan(0);
    assertThat(searchResultProto.getQueryStats().getNativeToJavaJniLatencyMs()).isAtLeast(0);
    assertThat(searchResultProto.getQueryStats().getJavaToNativeJniLatencyMs()).isAtLeast(0);
  }

  @Test
  public void testGetNextPage() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
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
      assertWithMessage(icingSearchEngine.put(emailDocument).getStatus().getMessage())
          .that(icingSearchEngine.put(emailDocument).getStatus().getCode())
          .isEqualTo(StatusProto.Code.OK);
    }

    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("foo")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();
    ResultSpecProto resultSpecProto = ResultSpecProto.newBuilder().setNumPerPage(1).build();

    SearchResultProto searchResultProto =
        icingSearchEngine.search(
            searchSpec, ScoringSpecProto.getDefaultInstance(), resultSpecProto);
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
    DocumentProto resultDocument = searchResultProto.getResults(0).getDocument();
    assertThat(resultDocument).isEqualTo(documents.remove(resultDocument.getUri()));

    assertThat(searchResultProto.getQueryStats().hasNativeToJavaStartTimestampMs()).isTrue();
    assertThat(searchResultProto.getQueryStats().hasNativeToJavaJniLatencyMs()).isTrue();
    assertThat(searchResultProto.getQueryStats().hasJavaToNativeJniLatencyMs()).isTrue();
    assertThat(searchResultProto.getQueryStats().getNativeToJavaStartTimestampMs())
        .isGreaterThan(0);
    assertThat(searchResultProto.getQueryStats().getNativeToJavaJniLatencyMs()).isAtLeast(0);
    assertThat(searchResultProto.getQueryStats().getJavaToNativeJniLatencyMs()).isAtLeast(0);

    // fetch rest pages
    for (int i = 1; i < 5; i++) {
      searchResultProto = icingSearchEngine.getNextPage(searchResultProto.getNextPageToken());
      assertWithMessage(searchResultProto.getStatus().getMessage())
          .that(searchResultProto.getStatus().getCode())
          .isEqualTo(StatusProto.Code.OK);
      assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
      resultDocument = searchResultProto.getResults(0).getDocument();
      assertThat(resultDocument).isEqualTo(documents.remove(resultDocument.getUri()));
    }

    // invalidate rest result
    icingSearchEngine.invalidateNextPageToken(searchResultProto.getNextPageToken());

    searchResultProto = icingSearchEngine.getNextPage(searchResultProto.getNextPageToken());
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(0);
  }

  @Ignore // b/350530146
  @Test
  public void writeAndReadBlob_blobContentMatches() throws Exception {
    // 1 Arrange: set up IcingSearchEngine with and blob data
    File tempDir = temporaryFolder.newFolder();
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder()
            .setBaseDir(tempDir.getCanonicalPath())
            .setEnableBlobStore(true)
            .build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    byte[] data = generateRandomBytes(100); // 10 Bytes
    byte[] digest = calculateDigest(data);
    PropertyProto.BlobHandleProto blobHandle =
        PropertyProto.BlobHandleProto.newBuilder()
            .setDigest(ByteString.copyFrom(digest))
            .setNamespace("namespace")
            .build();

    // 2 Act: write the blob and read it back.
    BlobProto openWriteBlobProto = icing.openWriteBlob(blobHandle);
    assertStatusOk(openWriteBlobProto.getStatus());
    Field field = FileDescriptor.class.getDeclaredField("fd");
    field.setAccessible(true); // Make the field accessible

    // Create a new FileDescriptor object
    FileDescriptor writeFd = new FileDescriptor();

    // Set the file descriptor value using reflection
    field.setInt(writeFd, openWriteBlobProto.getFileDescriptor());

    try (FileOutputStream outputStream = new FileOutputStream(writeFd)) {
      outputStream.write(data);
    }

    // Commit and read the blob.
    BlobProto commitBlobProto = icing.commitBlob(blobHandle);
    assertStatusOk(commitBlobProto.getStatus());

    BlobProto openReadBlobProto = icing.openReadBlob(blobHandle);
    assertStatusOk(openReadBlobProto.getStatus());

    FileDescriptor readFd = new FileDescriptor();
    field.setInt(readFd, openReadBlobProto.getFileDescriptor());
    byte[] output = new byte[data.length];
    try (FileInputStream inputStream = new FileInputStream(readFd)) {
      inputStream.read(output);
    }

    // 3 Assert: the blob content matches.
    assertThat(output).isEqualTo(data);
  }

  @Ignore // b/350530146
  @Test
  public void removeBlob() throws Exception {
    // 1 Arrange: set up IcingSearchEngine with and blob data
    File tempDir = temporaryFolder.newFolder();
    IcingSearchEngineOptions options =
        IcingSearchEngineOptions.newBuilder()
            .setBaseDir(tempDir.getCanonicalPath())
            .setEnableBlobStore(true)
            .build();
    IcingSearchEngine icing = new IcingSearchEngine(options);
    assertStatusOk(icing.initialize().getStatus());

    byte[] data = generateRandomBytes(100); // 10 Bytes
    byte[] digest = calculateDigest(data);
    PropertyProto.BlobHandleProto blobHandle =
        PropertyProto.BlobHandleProto.newBuilder()
            .setNamespace("ns")
            .setDigest(ByteString.copyFrom(digest))
            .build();

    // 2 Act: write the blob and read it back.
    BlobProto openWriteBlobProto = icing.openWriteBlob(blobHandle);
    assertStatusOk(openWriteBlobProto.getStatus());
    Field field = FileDescriptor.class.getDeclaredField("fd");
    field.setAccessible(true); // Make the field accessible

    // Create a new FileDescriptor object
    FileDescriptor writeFd = new FileDescriptor();

    // Set the file descriptor value using reflection
    field.setInt(writeFd, openWriteBlobProto.getFileDescriptor());

    try (FileOutputStream outputStream = new FileOutputStream(writeFd)) {
      outputStream.write(data);
    }

    // Remove the blob.
    BlobProto removeBlobProto = icing.removeBlob(blobHandle);
    assertStatusOk(removeBlobProto.getStatus());

    // Commit will not found.
    BlobProto commitBlobProto = icing.commitBlob(blobHandle);
    assertThat(commitBlobProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDelete() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icingSearchEngine.put(emailDocument).getStatus());

    DeleteResultProto deleteResultProto = icingSearchEngine.delete("namespace", "uri");
    assertStatusOk(deleteResultProto.getStatus());

    GetResultProto getResultProto =
        icingSearchEngine.get("namespace", "uri", GetResultSpecProto.getDefaultInstance());
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteByNamespace() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icingSearchEngine.put(emailDocument).getStatus());

    DeleteByNamespaceResultProto deleteByNamespaceResultProto =
        icingSearchEngine.deleteByNamespace("namespace");
    assertStatusOk(deleteByNamespaceResultProto.getStatus());

    GetResultProto getResultProto =
        icingSearchEngine.get("namespace", "uri", GetResultSpecProto.getDefaultInstance());
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteBySchemaType() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icingSearchEngine.put(emailDocument).getStatus());

    DeleteBySchemaTypeResultProto deleteBySchemaTypeResultProto =
        icingSearchEngine.deleteBySchemaType(EMAIL_TYPE);
    assertStatusOk(deleteBySchemaTypeResultProto.getStatus());

    GetResultProto getResultProto =
        icingSearchEngine.get("namespace", "uri", GetResultSpecProto.getDefaultInstance());
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
  }

  @Test
  public void testDeleteByQuery() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument1 =
        createEmailDocument("namespace", "uri1").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
            .build();

    assertStatusOk(icingSearchEngine.put(emailDocument1).getStatus());
    DocumentProto emailDocument2 =
        createEmailDocument("namespace", "uri2").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("bar"))
            .build();

    assertStatusOk(icingSearchEngine.put(emailDocument2).getStatus());

    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("foo")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();

    SearchResultProto searchResultProto =
        icingSearchEngine.search(
            searchSpec,
            ScoringSpecProto.getDefaultInstance(),
            ResultSpecProto.getDefaultInstance());
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);
    assertThat(searchResultProto.getResults(0).getDocument()).isEqualTo(emailDocument1);

    DeleteByQueryResultProto deleteResultProto = icingSearchEngine.deleteByQuery(searchSpec);
    assertStatusOk(deleteResultProto.getStatus());
    // By default, the deleteByQuery API does not return the summary about deleted documents, unless
    // the returnDeletedDocumentInfo parameter is set to true.
    assertThat(deleteResultProto.getDeletedDocumentsList()).isEmpty();

    GetResultProto getResultProto =
        icingSearchEngine.get("namespace", "uri1", GetResultSpecProto.getDefaultInstance());
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
    getResultProto =
        icingSearchEngine.get("namespace", "uri2", GetResultSpecProto.getDefaultInstance());
    assertStatusOk(getResultProto.getStatus());
  }

  @Test
  public void testDeleteByQueryWithDeletedDocumentInfo() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument1 =
        createEmailDocument("namespace", "uri1").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
            .build();

    assertStatusOk(icingSearchEngine.put(emailDocument1).getStatus());
    DocumentProto emailDocument2 =
        createEmailDocument("namespace", "uri2").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("bar"))
            .build();

    assertStatusOk(icingSearchEngine.put(emailDocument2).getStatus());

    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("foo")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();

    DeleteByQueryResultProto deleteResultProto =
        icingSearchEngine.deleteByQuery(searchSpec, /* returnDeletedDocumentInfo= */ true);
    assertStatusOk(deleteResultProto.getStatus());
    DeleteByQueryResultProto.DocumentGroupInfo info =
        DeleteByQueryResultProto.DocumentGroupInfo.newBuilder()
            .setNamespace("namespace")
            .setSchema("Email")
            .addUris("uri1")
            .build();
    assertThat(deleteResultProto.getDeletedDocumentsList()).containsExactly(info);

    GetResultProto getResultProto =
        icingSearchEngine.get("namespace", "uri1", GetResultSpecProto.getDefaultInstance());
    assertThat(getResultProto.getStatus().getCode()).isEqualTo(StatusProto.Code.NOT_FOUND);
    getResultProto =
        icingSearchEngine.get("namespace", "uri2", GetResultSpecProto.getDefaultInstance());
    assertStatusOk(getResultProto.getStatus());
  }

  @Test
  public void testPersistToDisk() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    PersistToDiskResultProto persistToDiskResultProto =
        icingSearchEngine.persistToDisk(PersistType.Code.LITE);
    assertStatusOk(persistToDiskResultProto.getStatus());
  }

  @Test
  public void testOptimize() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    OptimizeResultProto optimizeResultProto = icingSearchEngine.optimize();
    assertStatusOk(optimizeResultProto.getStatus());
  }

  @Test
  public void testGetOptimizeInfo() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    GetOptimizeInfoResultProto getOptimizeInfoResultProto = icingSearchEngine.getOptimizeInfo();
    assertStatusOk(getOptimizeInfoResultProto.getStatus());
    assertThat(getOptimizeInfoResultProto.getOptimizableDocs()).isEqualTo(0);
    assertThat(getOptimizeInfoResultProto.getEstimatedOptimizableBytes()).isEqualTo(0);
  }

  @Test
  public void testGetStorageInfo() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    StorageInfoResultProto storageInfoResultProto = icingSearchEngine.getStorageInfo();
    assertStatusOk(storageInfoResultProto.getStatus());
  }

  @Test
  public void testGetDebugInfo() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icingSearchEngine.put(emailDocument).getStatus());

    DebugInfoResultProto debugInfoResultProtoBasic =
        icingSearchEngine.getDebugInfo(DebugInfoVerbosity.Code.BASIC);
    assertStatusOk(debugInfoResultProtoBasic.getStatus());
    assertThat(debugInfoResultProtoBasic.getDebugInfo().getDocumentInfo().getCorpusInfoList())
        .isEmpty(); // because verbosity=BASIC

    DebugInfoResultProto debugInfoResultProtoDetailed =
        icingSearchEngine.getDebugInfo(DebugInfoVerbosity.Code.DETAILED);
    assertStatusOk(debugInfoResultProtoDetailed.getStatus());
    assertThat(debugInfoResultProtoDetailed.getDebugInfo().getDocumentInfo().getCorpusInfoList())
        .hasSize(1); // because verbosity=DETAILED
  }

  @Test
  public void testGetAllNamespaces() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    assertStatusOk(icingSearchEngine.put(emailDocument).getStatus());

    GetAllNamespacesResultProto getAllNamespacesResultProto = icingSearchEngine.getAllNamespaces();
    assertStatusOk(getAllNamespacesResultProto.getStatus());
    assertThat(getAllNamespacesResultProto.getNamespacesList()).containsExactly("namespace");
  }

  @Test
  public void testReset() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    ResetResultProto resetResultProto = icingSearchEngine.reset();
    assertStatusOk(resetResultProto.getStatus());
  }

  @Test
  public void testReportUsage() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    // Set schema and put a document.
    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument = createEmailDocument("namespace", "uri");
    PutResultProto putResultProto = icingSearchEngine.put(emailDocument);
    assertStatusOk(putResultProto.getStatus());

    // Report usage
    UsageReport usageReport =
        UsageReport.newBuilder()
            .setDocumentNamespace("namespace")
            .setDocumentUri("uri")
            .setUsageTimestampMs(1)
            .setUsageType(UsageReport.UsageType.USAGE_TYPE1)
            .build();
    ReportUsageResultProto reportUsageResultProto = icingSearchEngine.reportUsage(usageReport);
    assertStatusOk(reportUsageResultProto.getStatus());
  }

  @Test
  public void testCJKTSnippets() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaProto schema = SchemaProto.newBuilder().addTypes(createEmailTypeConfig()).build();
    assertStatusOk(
        icingSearchEngine
            .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
            .getStatus());

    // String:     "天是蓝的"
    //              ^ ^^ ^
    // UTF16 idx:   0 1 2 3
    // Breaks into segments: "天", "是", "蓝", "的"
    // "The sky is blue"
    String chinese = "天是蓝的";
    assertThat(chinese.length()).isEqualTo(4);
    DocumentProto emailDocument1 =
        createEmailDocument("namespace", "uri1").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues(chinese))
            .build();
    assertStatusOk(icingSearchEngine.put(emailDocument1).getStatus());

    // Search and request snippet matching but no windowing.
    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("是")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();
    ResultSpecProto resultSpecProto =
        ResultSpecProto.newBuilder()
            .setSnippetSpec(
                ResultSpecProto.SnippetSpecProto.newBuilder()
                    .setNumToSnippet(Integer.MAX_VALUE)
                    .setNumMatchesPerProperty(Integer.MAX_VALUE))
            .build();

    // Search and make sure that we got a single successful results
    SearchResultProto searchResultProto =
        icingSearchEngine.search(
            searchSpec, ScoringSpecProto.getDefaultInstance(), resultSpecProto);
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);

    // Ensure that one and only one property was matched and it was "subject"
    SnippetProto snippetProto = searchResultProto.getResults(0).getSnippet();
    assertThat(snippetProto.getEntriesList()).hasSize(1);
    SnippetProto.EntryProto entryProto = snippetProto.getEntries(0);
    assertThat(entryProto.getPropertyName()).isEqualTo("subject");

    // Get the content for "subject" and see what the match is.
    DocumentProto resultDocument = searchResultProto.getResults(0).getDocument();
    assertThat(resultDocument.getPropertiesList()).hasSize(1);
    PropertyProto subjectProperty = resultDocument.getProperties(0);
    assertThat(subjectProperty.getName()).isEqualTo("subject");
    assertThat(subjectProperty.getStringValuesList()).hasSize(1);
    String content = subjectProperty.getStringValues(0);

    // Ensure that there is one and only one match within "subject"
    assertThat(entryProto.getSnippetMatchesList()).hasSize(1);
    SnippetMatchProto matchProto = entryProto.getSnippetMatches(0);

    int matchStart = matchProto.getExactMatchUtf16Position();
    int matchEnd = matchStart + matchProto.getExactMatchUtf16Length();
    assertThat(matchStart).isEqualTo(1);
    assertThat(matchEnd).isEqualTo(2);
    String match = content.substring(matchStart, matchEnd);
    assertThat(match).isEqualTo("是");
  }

  @Test
  public void testUtf16MultiByteSnippets() throws Exception {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaProto schema = SchemaProto.newBuilder().addTypes(createEmailTypeConfig()).build();
    assertStatusOk(
        icingSearchEngine
            .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
            .getStatus());

    // String:    "𐀀𐀁 𐀂𐀃 𐀄"
    //             ^  ^  ^
    // UTF16 idx:  0  5  10
    // Breaks into segments: "𐀀𐀁", "𐀂𐀃", "𐀄"
    String text = "𐀀𐀁 𐀂𐀃 𐀄";
    assertThat(text.length()).isEqualTo(12);
    DocumentProto emailDocument1 =
        createEmailDocument("namespace", "uri1").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues(text))
            .build();
    assertStatusOk(icingSearchEngine.put(emailDocument1).getStatus());

    // Search and request snippet matching but no windowing.
    SearchSpecProto searchSpec =
        SearchSpecProto.newBuilder()
            .setQuery("𐀂")
            .setTermMatchType(TermMatchType.Code.PREFIX)
            .build();
    ResultSpecProto resultSpecProto =
        ResultSpecProto.newBuilder()
            .setSnippetSpec(
                ResultSpecProto.SnippetSpecProto.newBuilder()
                    .setNumToSnippet(Integer.MAX_VALUE)
                    .setNumMatchesPerProperty(Integer.MAX_VALUE))
            .build();

    // Search and make sure that we got a single successful results
    SearchResultProto searchResultProto =
        icingSearchEngine.search(
            searchSpec, ScoringSpecProto.getDefaultInstance(), resultSpecProto);
    assertStatusOk(searchResultProto.getStatus());
    assertThat(searchResultProto.getResultsCount()).isEqualTo(1);

    // Ensure that one and only one property was matched and it was "subject"
    SnippetProto snippetProto = searchResultProto.getResults(0).getSnippet();
    assertThat(snippetProto.getEntriesList()).hasSize(1);
    SnippetProto.EntryProto entryProto = snippetProto.getEntries(0);
    assertThat(entryProto.getPropertyName()).isEqualTo("subject");

    // Get the content for "subject" and see what the match is.
    DocumentProto resultDocument = searchResultProto.getResults(0).getDocument();
    assertThat(resultDocument.getPropertiesList()).hasSize(1);
    PropertyProto subjectProperty = resultDocument.getProperties(0);
    assertThat(subjectProperty.getName()).isEqualTo("subject");
    assertThat(subjectProperty.getStringValuesList()).hasSize(1);
    String content = subjectProperty.getStringValues(0);

    // Ensure that there is one and only one match within "subject"
    assertThat(entryProto.getSnippetMatchesList()).hasSize(1);
    SnippetMatchProto matchProto = entryProto.getSnippetMatches(0);

    int matchStart = matchProto.getExactMatchUtf16Position();
    int matchEnd = matchStart + matchProto.getExactMatchUtf16Length();
    assertThat(matchStart).isEqualTo(5);
    assertThat(matchEnd).isEqualTo(9);
    String match = content.substring(matchStart, matchEnd);
    assertThat(match).isEqualTo("𐀂𐀃");
  }

  @Test
  public void testSearchSuggestions() {
    assertStatusOk(icingSearchEngine.initialize().getStatus());

    SchemaTypeConfigProto emailTypeConfig = createEmailTypeConfig();
    SchemaProto schema = SchemaProto.newBuilder().addTypes(emailTypeConfig).build();
    assertThat(
            icingSearchEngine
                .setSchema(schema, /* ignoreErrorsAndDeleteDocuments= */ false)
                .getStatus()
                .getCode())
        .isEqualTo(StatusProto.Code.OK);

    DocumentProto emailDocument1 =
        createEmailDocument("namespace", "uri1").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("fo"))
            .build();
    DocumentProto emailDocument2 =
        createEmailDocument("namespace", "uri2").toBuilder()
            .addProperties(PropertyProto.newBuilder().setName("subject").addStringValues("foo"))
            .build();
    assertStatusOk(icingSearchEngine.put(emailDocument1).getStatus());
    assertStatusOk(icingSearchEngine.put(emailDocument2).getStatus());

    SuggestionSpecProto suggestionSpec =
        SuggestionSpecProto.newBuilder()
            .setPrefix("f")
            .setNumToReturn(10)
            .setScoringSpec(
                SuggestionScoringSpecProto.newBuilder()
                    .setScoringMatchType(Code.EXACT_ONLY)
                    .build())
            .build();

    SuggestionResponse response = icingSearchEngine.searchSuggestions(suggestionSpec);
    assertStatusOk(response.getStatus());
    assertThat(response.getSuggestionsList()).hasSize(2);
    assertThat(response.getSuggestions(0).getQuery()).isEqualTo("foo");
    assertThat(response.getSuggestions(1).getQuery()).isEqualTo("fo");
  }

  @Test
  public void testLogging() throws Exception {
    // Set to INFO
    assertThat(IcingSearchEngine.setLoggingLevel(LogSeverity.Code.INFO)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.INFO)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.DBG)).isFalse();

    // Set to WARNING
    assertThat(IcingSearchEngine.setLoggingLevel(LogSeverity.Code.WARNING)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.WARNING)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.INFO)).isFalse();

    // Set to DEBUG
    assertThat(IcingSearchEngine.setLoggingLevel(LogSeverity.Code.DBG)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.DBG)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.VERBOSE)).isFalse();

    // Set to VERBOSE
    assertThat(IcingSearchEngine.setLoggingLevel(LogSeverity.Code.VERBOSE, (short) 1)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.VERBOSE, (short) 1)).isTrue();
    assertThat(IcingSearchEngine.shouldLog(LogSeverity.Code.VERBOSE, (short) 2)).isFalse();

    assertThat(IcingSearchEngine.getLoggingTag()).isNotEmpty();
  }

  private static void assertStatusOk(StatusProto status) {
    assertWithMessage(status.getMessage()).that(status.getCode()).isEqualTo(StatusProto.Code.OK);
  }
}
