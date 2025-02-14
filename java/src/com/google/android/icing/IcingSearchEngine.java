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
import com.google.android.icing.proto.PropertyProto;
import com.google.android.icing.proto.PutResultProto;
import com.google.android.icing.proto.ReportUsageResultProto;
import com.google.android.icing.proto.ResetResultProto;
import com.google.android.icing.proto.ResultSpecProto;
import com.google.android.icing.proto.SchemaProto;
import com.google.android.icing.proto.ScoringSpecProto;
import com.google.android.icing.proto.SearchResultProto;
import com.google.android.icing.proto.SearchSpecProto;
import com.google.android.icing.proto.SetSchemaResultProto;
import com.google.android.icing.proto.StorageInfoResultProto;
import com.google.android.icing.proto.SuggestionResponse;
import com.google.android.icing.proto.SuggestionSpecProto;
import com.google.android.icing.proto.UsageReport;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Java wrapper to access {@link IcingSearchEngineImpl}.
 *
 * <p>It converts byte array from {@link IcingSearchEngineImpl} to corresponding protos.
 *
 * <p>If this instance has been closed, the instance is no longer usable.
 *
 * <p>Keep this class to be non-Final so that it can be mocked in AppSearch.
 *
 * <p>NOTE: This class is NOT thread-safe.
 */
public class IcingSearchEngine implements IcingSearchEngineInterface {

  private static final String TAG = "IcingSearchEngine";
  private final IcingSearchEngineImpl icingSearchEngineImpl;

  /**
   * @throws IllegalStateException if IcingSearchEngine fails to be created
   */
  public IcingSearchEngine(@NonNull IcingSearchEngineOptions options) {
    icingSearchEngineImpl = new IcingSearchEngineImpl(options.toByteArray());
  }

  @Override
  public void close() {
    icingSearchEngineImpl.close();
  }

  @Override
  public @NonNull InitializeResultProto initialize() {
    return IcingSearchEngineUtils.byteArrayToInitializeResultProto(
        icingSearchEngineImpl.initialize());
  }

  @Override
  public @NonNull SetSchemaResultProto setSchema(@NonNull SchemaProto schema) {
    return setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);
  }

  @Override
  public @NonNull SetSchemaResultProto setSchema(
      @NonNull SchemaProto schema, boolean ignoreErrorsAndDeleteDocuments) {
    return IcingSearchEngineUtils.byteArrayToSetSchemaResultProto(
        icingSearchEngineImpl.setSchema(schema.toByteArray(), ignoreErrorsAndDeleteDocuments));
  }

  @Override
  public @NonNull GetSchemaResultProto getSchema() {
    return IcingSearchEngineUtils.byteArrayToGetSchemaResultProto(
        icingSearchEngineImpl.getSchema());
  }

  @Override
  public @NonNull GetSchemaResultProto getSchemaForDatabase(@NonNull String database) {
    return IcingSearchEngineUtils.byteArrayToGetSchemaResultProto(
        icingSearchEngineImpl.getSchemaForDatabase(database));
  }

  @Override
  public @NonNull GetSchemaTypeResultProto getSchemaType(@NonNull String schemaType) {
    return IcingSearchEngineUtils.byteArrayToGetSchemaTypeResultProto(
        icingSearchEngineImpl.getSchemaType(schemaType));
  }

  @Override
  public @NonNull PutResultProto put(@NonNull DocumentProto document) {
    return IcingSearchEngineUtils.byteArrayToPutResultProto(
        icingSearchEngineImpl.put(document.toByteArray()));
  }

  @Override
  public @NonNull GetResultProto get(
      @NonNull String namespace, @NonNull String uri, @NonNull GetResultSpecProto getResultSpec) {
    return IcingSearchEngineUtils.byteArrayToGetResultProto(
        icingSearchEngineImpl.get(namespace, uri, getResultSpec.toByteArray()));
  }

  @Override
  public @NonNull ReportUsageResultProto reportUsage(@NonNull UsageReport usageReport) {
    return IcingSearchEngineUtils.byteArrayToReportUsageResultProto(
        icingSearchEngineImpl.reportUsage(usageReport.toByteArray()));
  }

  @Override
  public @NonNull GetAllNamespacesResultProto getAllNamespaces() {
    return IcingSearchEngineUtils.byteArrayToGetAllNamespacesResultProto(
        icingSearchEngineImpl.getAllNamespaces());
  }

  @Override
  public @NonNull SearchResultProto search(
      @NonNull SearchSpecProto searchSpec,
      @NonNull ScoringSpecProto scoringSpec,
      @NonNull ResultSpecProto resultSpec) {
    return IcingSearchEngineUtils.byteArrayToSearchResultProto(
        icingSearchEngineImpl.search(
            searchSpec.toByteArray(), scoringSpec.toByteArray(), resultSpec.toByteArray()));
  }

  @Override
  public @NonNull SearchResultProto getNextPage(long nextPageToken) {
    return IcingSearchEngineUtils.byteArrayToSearchResultProto(
        icingSearchEngineImpl.getNextPage(nextPageToken));
  }

  @Override
  public void invalidateNextPageToken(long nextPageToken) {
    icingSearchEngineImpl.invalidateNextPageToken(nextPageToken);
  }

  @Override
  public @NonNull BlobProto openWriteBlob(PropertyProto.@NonNull BlobHandleProto blobHandle) {
    return IcingSearchEngineUtils.byteArrayToBlobProto(
        icingSearchEngineImpl.openWriteBlob(blobHandle.toByteArray()));
  }

  @Override
  public @NonNull BlobProto removeBlob(PropertyProto.@NonNull BlobHandleProto blobHandle) {
    return IcingSearchEngineUtils.byteArrayToBlobProto(
        icingSearchEngineImpl.removeBlob(blobHandle.toByteArray()));
  }

  @Override
  public @NonNull BlobProto openReadBlob(PropertyProto.@NonNull BlobHandleProto blobHandle) {
    return IcingSearchEngineUtils.byteArrayToBlobProto(
        icingSearchEngineImpl.openReadBlob(blobHandle.toByteArray()));
  }

  @Override
  public @NonNull BlobProto commitBlob(PropertyProto.@NonNull BlobHandleProto blobHandle) {
    return IcingSearchEngineUtils.byteArrayToBlobProto(
        icingSearchEngineImpl.commitBlob(blobHandle.toByteArray()));
  }

  @Override
  public @NonNull DeleteResultProto delete(@NonNull String namespace, @NonNull String uri) {
    return IcingSearchEngineUtils.byteArrayToDeleteResultProto(
        icingSearchEngineImpl.delete(namespace, uri));
  }

  @Override
  public @NonNull SuggestionResponse searchSuggestions(
          @NonNull SuggestionSpecProto suggestionSpec) {
    return IcingSearchEngineUtils.byteArrayToSuggestionResponse(
        icingSearchEngineImpl.searchSuggestions(suggestionSpec.toByteArray()));
  }

  @Override
  public @NonNull DeleteByNamespaceResultProto deleteByNamespace(@NonNull String namespace) {
    return IcingSearchEngineUtils.byteArrayToDeleteByNamespaceResultProto(
        icingSearchEngineImpl.deleteByNamespace(namespace));
  }

  @Override
  public @NonNull DeleteBySchemaTypeResultProto deleteBySchemaType(@NonNull String schemaType) {
    return IcingSearchEngineUtils.byteArrayToDeleteBySchemaTypeResultProto(
        icingSearchEngineImpl.deleteBySchemaType(schemaType));
  }

  @Override
  public @NonNull DeleteByQueryResultProto deleteByQuery(@NonNull SearchSpecProto searchSpec) {
    return deleteByQuery(searchSpec, /*returnDeletedDocumentInfo=*/ false);
  }

  @Override
  public @NonNull DeleteByQueryResultProto deleteByQuery(
      @NonNull SearchSpecProto searchSpec, boolean returnDeletedDocumentInfo) {
    return IcingSearchEngineUtils.byteArrayToDeleteByQueryResultProto(
        icingSearchEngineImpl.deleteByQuery(searchSpec.toByteArray(), returnDeletedDocumentInfo));
  }

  @Override
  public @NonNull PersistToDiskResultProto persistToDisk(
          PersistType.@NonNull Code persistTypeCode) {
    return IcingSearchEngineUtils.byteArrayToPersistToDiskResultProto(
        icingSearchEngineImpl.persistToDisk(persistTypeCode.getNumber()));
  }

  @Override
  public @NonNull OptimizeResultProto optimize() {
    return IcingSearchEngineUtils.byteArrayToOptimizeResultProto(icingSearchEngineImpl.optimize());
  }

  @Override
  public @NonNull GetOptimizeInfoResultProto getOptimizeInfo() {
    return IcingSearchEngineUtils.byteArrayToGetOptimizeInfoResultProto(
        icingSearchEngineImpl.getOptimizeInfo());
  }

  @Override
  public @NonNull StorageInfoResultProto getStorageInfo() {
    return IcingSearchEngineUtils.byteArrayToStorageInfoResultProto(
        icingSearchEngineImpl.getStorageInfo());
  }

  @Override
  public @NonNull DebugInfoResultProto getDebugInfo(DebugInfoVerbosity.@NonNull Code verbosity) {
    return IcingSearchEngineUtils.byteArrayToDebugInfoResultProto(
        icingSearchEngineImpl.getDebugInfo(verbosity.getNumber()));
  }

  @Override
  public @NonNull ResetResultProto reset() {
    return IcingSearchEngineUtils.byteArrayToResetResultProto(icingSearchEngineImpl.reset());
  }

  public static boolean shouldLog(LogSeverity.Code severity) {
    return shouldLog(severity, (short) 0);
  }

  public static boolean shouldLog(LogSeverity.Code severity, short verbosity) {
    return IcingSearchEngineImpl.shouldLog((short) severity.getNumber(), verbosity);
  }

  public static boolean setLoggingLevel(LogSeverity.Code severity) {
    return setLoggingLevel(severity, (short) 0);
  }

  public static boolean setLoggingLevel(LogSeverity.Code severity, short verbosity) {
    return IcingSearchEngineImpl.setLoggingLevel((short) severity.getNumber(), verbosity);
  }

  public static @Nullable String getLoggingTag() {
    return IcingSearchEngineImpl.getLoggingTag();
  }
}
