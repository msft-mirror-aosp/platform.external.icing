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

import android.util.Log;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import com.google.android.icing.proto.BatchGetResultProto;
import com.google.android.icing.proto.BatchPutResultProto;
import com.google.android.icing.proto.BlobProto;
import com.google.android.icing.proto.DebugInfoResultProto;
import com.google.android.icing.proto.DeleteByNamespaceResultProto;
import com.google.android.icing.proto.DeleteByQueryResultProto;
import com.google.android.icing.proto.DeleteBySchemaTypeResultProto;
import com.google.android.icing.proto.DeleteResultProto;
import com.google.android.icing.proto.GetAllNamespacesResultProto;
import com.google.android.icing.proto.GetOptimizeInfoResultProto;
import com.google.android.icing.proto.GetResultProto;
import com.google.android.icing.proto.GetSchemaResultProto;
import com.google.android.icing.proto.GetSchemaTypeResultProto;
import com.google.android.icing.proto.HandleExpiredDocumentsResultProto;
import com.google.android.icing.proto.InitializeResultProto;
import com.google.android.icing.proto.MaintainAnnIndexResultProto;
import com.google.android.icing.proto.OptimizeResultProto;
import com.google.android.icing.proto.PersistToDiskResultProto;
import com.google.android.icing.proto.PutResultProto;
import com.google.android.icing.proto.ReportUsageResultProto;
import com.google.android.icing.proto.ResetResultProto;
import com.google.android.icing.proto.SearchResultProto;
import com.google.android.icing.proto.SetSchemaResultProto;
import com.google.android.icing.proto.StatusProto;
import com.google.android.icing.proto.StorageInfoResultProto;
import com.google.android.icing.proto.SuggestionResponse;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import java.util.function.Function;

/**
 * Contains utility methods for IcingSearchEngine to convert byte arrays to the corresponding
 * protos.
 *
 * <p>It is also being used by AppSearch dynamite 0p client APIs to convert byte arrays to the
 * protos.
 */
// TODO(b/347054358): Add unit tests for this class.
public final class IcingSearchEngineUtils {
  private static final String TAG = "IcingSearchEngineUtils";
  private static final ExtensionRegistryLite EXTENSION_REGISTRY_LITE =
      ExtensionRegistryLite.getEmptyRegistry();

  private IcingSearchEngineUtils() {}

  // TODO(b/240333360) Check to see if we can use one template function to replace those
  @NonNull
  public static InitializeResultProto byteArrayToInitializeResultProto(
      @Nullable byte[] initializeResultBytes) {
    InitializeResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            initializeResultBytes,
            InitializeResultProto.newBuilder(),
            status -> InitializeResultProto.newBuilder().setStatus(status));
    if (initializeResultBytes != null) {
      builder.setResponseBytes(initializeResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static SetSchemaResultProto byteArrayToSetSchemaResultProto(
      @Nullable byte[] setSchemaResultBytes, int requestSize) {
    SetSchemaResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                setSchemaResultBytes,
                SetSchemaResultProto.newBuilder(),
                status -> SetSchemaResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (setSchemaResultBytes != null) {
      builder.setResponseBytes(setSchemaResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static GetSchemaResultProto byteArrayToGetSchemaResultProto(
      @Nullable byte[] getSchemaResultBytes) {
    GetSchemaResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            getSchemaResultBytes,
            GetSchemaResultProto.newBuilder(),
            status -> GetSchemaResultProto.newBuilder().setStatus(status));
    if (getSchemaResultBytes != null) {
      builder.setResponseBytes(getSchemaResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static GetSchemaTypeResultProto byteArrayToGetSchemaTypeResultProto(
      @Nullable byte[] getSchemaTypeResultBytes) {
    GetSchemaTypeResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            getSchemaTypeResultBytes,
            GetSchemaTypeResultProto.newBuilder(),
            status -> GetSchemaTypeResultProto.newBuilder().setStatus(status));
    if (getSchemaTypeResultBytes != null) {
      builder.setResponseBytes(getSchemaTypeResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static PutResultProto byteArrayToPutResultProto(
      @Nullable byte[] putResultBytes, int requestSize) {
    PutResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                putResultBytes,
                PutResultProto.newBuilder(),
                status -> PutResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (putResultBytes != null) {
      builder.setResponseBytes(putResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static BatchPutResultProto byteArrayToBatchPutResultProto(
      @Nullable byte[] putResultsBytes, int requestSize) {
    BatchPutResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                putResultsBytes,
                BatchPutResultProto.newBuilder(),
                status -> BatchPutResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (putResultsBytes != null) {
      builder.setResponseBytes(putResultsBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static GetResultProto byteArrayToGetResultProto(
      @Nullable byte[] getResultBytes, int requestSize) {
    GetResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                getResultBytes,
                GetResultProto.newBuilder(),
                status -> GetResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (getResultBytes != null) {
      builder.setResponseBytes(getResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static BatchGetResultProto byteArrayToBatchGetResultProto(
      @Nullable byte[] batchGetResultBytes, int requestSize) {
    BatchGetResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                batchGetResultBytes,
                BatchGetResultProto.newBuilder(),
                status -> BatchGetResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (batchGetResultBytes != null) {
      builder.setResponseBytes(batchGetResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static ReportUsageResultProto byteArrayToReportUsageResultProto(
      @Nullable byte[] reportUsageResultBytes, int requestSize) {
    ReportUsageResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                reportUsageResultBytes,
                ReportUsageResultProto.newBuilder(),
                status -> ReportUsageResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (reportUsageResultBytes != null) {
      builder.setResponseBytes(reportUsageResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static GetAllNamespacesResultProto byteArrayToGetAllNamespacesResultProto(
      @Nullable byte[] getAllNamespacesResultBytes) {
    GetAllNamespacesResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            getAllNamespacesResultBytes,
            GetAllNamespacesResultProto.newBuilder(),
            status -> GetAllNamespacesResultProto.newBuilder().setStatus(status));
    if (getAllNamespacesResultBytes != null) {
      builder.setResponseBytes(getAllNamespacesResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static SearchResultProto byteArrayToSearchResultProto(
      @Nullable byte[] searchResultBytes, int requestSize) {
    SearchResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                searchResultBytes,
                SearchResultProto.newBuilder(),
                status -> SearchResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (searchResultBytes != null) {
      builder.setResponseBytes(searchResultBytes.length);
      setNativeToJavaJniLatency(builder);
    }
    return builder.build();
  }

  /**
   * Converts a byte array to a {@link HandleExpiredDocumentsResultProto}.
   *
   * @param handleExpiredDocumentsResultBytes the byte array to convert
   * @return the {@link HandleExpiredDocumentsResultProto}
   */
  @NonNull
  public static HandleExpiredDocumentsResultProto byteArrayToHandleExpiredDocumentsResultProto(
      @Nullable byte[] handleExpiredDocumentsResultBytes) {
    HandleExpiredDocumentsResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            handleExpiredDocumentsResultBytes,
            HandleExpiredDocumentsResultProto.newBuilder(),
            status -> HandleExpiredDocumentsResultProto.newBuilder().setStatus(status));
    if (handleExpiredDocumentsResultBytes != null) {
      builder.setResponseBytes(handleExpiredDocumentsResultBytes.length);
    }
    return builder.build();
  }

  /**
   * Converts a byte array to a {@link MaintainAnnIndexResultProto}.
   *
   * @param maintainAnnIndexResultBytes the byte array to convert
   * @return the {@link MaintainAnnIndexResultProto}
   */
  @NonNull
  public static MaintainAnnIndexResultProto byteArrayToMaintainAnnIndexResultProto(
      @Nullable byte[] maintainAnnIndexResultBytes, int requestSize) {
    MaintainAnnIndexResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                maintainAnnIndexResultBytes,
                MaintainAnnIndexResultProto.newBuilder(),
                status -> MaintainAnnIndexResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (maintainAnnIndexResultBytes != null) {
      builder.setResponseBytes(maintainAnnIndexResultBytes.length);
    }
    return builder.build();
  }

  /**
   * Converts a byte array to a {@link BlobProto}.
   *
   * @param blobBytes the byte array to convert
   * @return the {@link BlobProto}
   */
  @NonNull
  public static BlobProto byteArrayToBlobProto(@Nullable byte[] blobBytes) {
    if (blobBytes == null) {
      Log.e(TAG, "Received null BlobProto from native.");
      return BlobProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return BlobProto.newBuilder().mergeFrom(blobBytes, EXTENSION_REGISTRY_LITE).build();
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing BlobProto.", e);
      return BlobProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  private static void setNativeToJavaJniLatency(
      SearchResultProto.Builder searchResultProtoBuilder) {
    int nativeToJavaLatencyMs =
        (int)
            (System.currentTimeMillis()
                - searchResultProtoBuilder.getQueryStats().getNativeToJavaStartTimestampMs());
    searchResultProtoBuilder.setQueryStats(
        searchResultProtoBuilder.getQueryStats().toBuilder()
            .setNativeToJavaJniLatencyMs(nativeToJavaLatencyMs));
  }

  @NonNull
  public static DeleteResultProto byteArrayToDeleteResultProto(@Nullable byte[] deleteResultBytes) {
    DeleteResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            deleteResultBytes,
            DeleteResultProto.newBuilder(),
            status -> DeleteResultProto.newBuilder().setStatus(status));
    if (deleteResultBytes != null) {
      builder.setResponseBytes(deleteResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static SuggestionResponse byteArrayToSuggestionResponse(
      @Nullable byte[] suggestionResponseBytes, int requestSize) {
    SuggestionResponse.Builder builder =
        getResponseProtoBuilderFromRawData(
                suggestionResponseBytes,
                SuggestionResponse.newBuilder(),
                status -> SuggestionResponse.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (suggestionResponseBytes != null) {
      builder.setResponseBytes(suggestionResponseBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static DeleteByNamespaceResultProto byteArrayToDeleteByNamespaceResultProto(
      @Nullable byte[] deleteByNamespaceResultBytes) {
    DeleteByNamespaceResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            deleteByNamespaceResultBytes,
            DeleteByNamespaceResultProto.newBuilder(),
            status -> DeleteByNamespaceResultProto.newBuilder().setStatus(status));
    if (deleteByNamespaceResultBytes != null) {
      builder.setResponseBytes(deleteByNamespaceResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static DeleteBySchemaTypeResultProto byteArrayToDeleteBySchemaTypeResultProto(
      @Nullable byte[] deleteBySchemaTypeResultBytes) {
    DeleteBySchemaTypeResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            deleteBySchemaTypeResultBytes,
            DeleteBySchemaTypeResultProto.newBuilder(),
            status -> DeleteBySchemaTypeResultProto.newBuilder().setStatus(status));
    if (deleteBySchemaTypeResultBytes != null) {
      builder.setResponseBytes(deleteBySchemaTypeResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static DeleteByQueryResultProto byteArrayToDeleteByQueryResultProto(
      @Nullable byte[] deleteResultBytes, int requestSize) {
    DeleteByQueryResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
                deleteResultBytes,
                DeleteByQueryResultProto.newBuilder(),
                status -> DeleteByQueryResultProto.newBuilder().setStatus(status))
            .setRequestBytes(requestSize);
    if (deleteResultBytes != null) {
      builder.setResponseBytes(deleteResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static PersistToDiskResultProto byteArrayToPersistToDiskResultProto(
      @Nullable byte[] persistToDiskResultBytes) {
    PersistToDiskResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            persistToDiskResultBytes,
            PersistToDiskResultProto.newBuilder(),
            status -> PersistToDiskResultProto.newBuilder().setStatus(status));
    if (persistToDiskResultBytes != null) {
      builder.setResponseBytes(persistToDiskResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static OptimizeResultProto byteArrayToOptimizeResultProto(
      @Nullable byte[] optimizeResultBytes) {
    OptimizeResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            optimizeResultBytes,
            OptimizeResultProto.newBuilder(),
            status -> OptimizeResultProto.newBuilder().setStatus(status));
    if (optimizeResultBytes != null) {
      builder.setResponseBytes(optimizeResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static GetOptimizeInfoResultProto byteArrayToGetOptimizeInfoResultProto(
      @Nullable byte[] getOptimizeInfoResultBytes) {
    GetOptimizeInfoResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            getOptimizeInfoResultBytes,
            GetOptimizeInfoResultProto.newBuilder(),
            status -> GetOptimizeInfoResultProto.newBuilder().setStatus(status));
    if (getOptimizeInfoResultBytes != null) {
      builder.setResponseBytes(getOptimizeInfoResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static StorageInfoResultProto byteArrayToStorageInfoResultProto(
      @Nullable byte[] storageInfoResultProtoBytes) {
    StorageInfoResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            storageInfoResultProtoBytes,
            StorageInfoResultProto.newBuilder(),
            status -> StorageInfoResultProto.newBuilder().setStatus(status));
    if (storageInfoResultProtoBytes != null) {
      builder.setResponseBytes(storageInfoResultProtoBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static DebugInfoResultProto byteArrayToDebugInfoResultProto(
      @Nullable byte[] debugInfoResultProtoBytes) {
    DebugInfoResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            debugInfoResultProtoBytes,
            DebugInfoResultProto.newBuilder(),
            status -> DebugInfoResultProto.newBuilder().setStatus(status));
    if (debugInfoResultProtoBytes != null) {
      builder.setResponseBytes(debugInfoResultProtoBytes.length);
    }
    return builder.build();
  }

  @NonNull
  public static ResetResultProto byteArrayToResetResultProto(@Nullable byte[] resetResultBytes) {
    ResetResultProto.Builder builder =
        getResponseProtoBuilderFromRawData(
            resetResultBytes,
            ResetResultProto.newBuilder(),
            status -> ResetResultProto.newBuilder().setStatus(status));
    if (resetResultBytes != null) {
      builder.setResponseBytes(resetResultBytes.length);
    }
    return builder.build();
  }

  @NonNull
  private static <B extends MessageLite.Builder> B getResponseProtoBuilderFromRawData(
      @Nullable byte[] result,
      @NonNull B builder,
      @NonNull Function<StatusProto, B> createResponseWithStatus) {
    if (result == null) {
      return createResponseWithStatus.apply(
          StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL).build());
    }

    try {
      builder.mergeFrom(result, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      return createResponseWithStatus.apply(
          StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL).build());
    }

    return builder;
  }
}
