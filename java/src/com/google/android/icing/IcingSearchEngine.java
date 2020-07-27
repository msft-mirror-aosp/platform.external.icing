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
import com.google.android.icing.proto.PutResultProto;
import com.google.android.icing.proto.ResetResultProto;
import com.google.android.icing.proto.ResultSpecProto;
import com.google.android.icing.proto.SchemaProto;
import com.google.android.icing.proto.ScoringSpecProto;
import com.google.android.icing.proto.SearchResultProto;
import com.google.android.icing.proto.SearchSpecProto;
import com.google.android.icing.proto.SetSchemaResultProto;
import com.google.android.icing.proto.StatusProto;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;

/** Java wrapper to access native APIs in external/icing/icing/icing-search-engine.h */
public final class IcingSearchEngine {

  private static final String TAG = "IcingSearchEngine";
  private static final ExtensionRegistryLite EXTENSION_REGISTRY_LITE =
      ExtensionRegistryLite.getEmptyRegistry();

  private final long nativePointer;

  static {
    // NOTE: This can fail with an UnsatisfiedLinkError
    System.loadLibrary("icing");
  }

  /** @throws IllegalStateException if IcingSearchEngine fails to be created */
  public IcingSearchEngine(@NonNull IcingSearchEngineOptions options) {
    nativePointer = nativeCreate(options.toByteArray());
    if (nativePointer == 0) {
      Log.e(TAG, "Failed to create IcingSearchEngine.");
      throw new IllegalStateException("Failed to create IcingSearchEngine.");
    }
  }

  @NonNull
  public InitializeResultProto initialize() {
    byte[] initializeResultBytes = nativeInitialize(nativePointer);
    if (initializeResultBytes == null) {
      Log.e(TAG, "Received null InitializeResult from native.");
      return InitializeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return InitializeResultProto.parseFrom(initializeResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing InitializeResultProto.", e);
      return InitializeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public SetSchemaResultProto setSchema(@NonNull SchemaProto schema) {
    return setSchema(schema, /*ignoreErrorsAndDeleteDocuments=*/ false);
  }

  @NonNull
  public SetSchemaResultProto setSchema(
      @NonNull SchemaProto schema, boolean ignoreErrorsAndDeleteDocuments) {
    byte[] setSchemaResultBytes =
        nativeSetSchema(nativePointer, schema.toByteArray(), ignoreErrorsAndDeleteDocuments);
    if (setSchemaResultBytes == null) {
      Log.e(TAG, "Received null SetSchemaResultProto from native.");
      return SetSchemaResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return SetSchemaResultProto.parseFrom(setSchemaResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing SetSchemaResultProto.", e);
      return SetSchemaResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public GetSchemaResultProto getSchema() {
    byte[] getSchemaResultBytes = nativeGetSchema(nativePointer);
    if (getSchemaResultBytes == null) {
      Log.e(TAG, "Received null GetSchemaResultProto from native.");
      return GetSchemaResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return GetSchemaResultProto.parseFrom(getSchemaResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing GetSchemaResultProto.", e);
      return GetSchemaResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public GetSchemaTypeResultProto getSchemaType(@NonNull String schemaType) {
    byte[] getSchemaTypeResultBytes = nativeGetSchemaType(nativePointer, schemaType);
    if (getSchemaTypeResultBytes == null) {
      Log.e(TAG, "Received null GetSchemaTypeResultProto from native.");
      return GetSchemaTypeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return GetSchemaTypeResultProto.parseFrom(getSchemaTypeResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing GetSchemaTypeResultProto.", e);
      return GetSchemaTypeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public PutResultProto put(@NonNull DocumentProto document) {
    byte[] putResultBytes = nativePut(nativePointer, document.toByteArray());
    if (putResultBytes == null) {
      Log.e(TAG, "Received null PutResultProto from native.");
      return PutResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return PutResultProto.parseFrom(putResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing PutResultProto.", e);
      return PutResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public GetResultProto get(@NonNull String namespace, @NonNull String uri) {
    byte[] getResultBytes = nativeGet(nativePointer, namespace, uri);
    if (getResultBytes == null) {
      Log.e(TAG, "Received null GetResultProto from native.");
      return GetResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return GetResultProto.parseFrom(getResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing GetResultProto.", e);
      return GetResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public GetAllNamespacesResultProto getAllNamespaces() {
    byte[] getAllNamespacesResultBytes = nativeGetAllNamespaces(nativePointer);
    if (getAllNamespacesResultBytes == null) {
      Log.e(TAG, "Received null GetAllNamespacesResultProto from native.");
      return GetAllNamespacesResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return GetAllNamespacesResultProto.parseFrom(
          getAllNamespacesResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing GetAllNamespacesResultProto.", e);
      return GetAllNamespacesResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public SearchResultProto search(
      @NonNull SearchSpecProto searchSpec,
      @NonNull ScoringSpecProto scoringSpec,
      @NonNull ResultSpecProto resultSpec) {
    byte[] searchResultBytes =
        nativeSearch(
            nativePointer,
            searchSpec.toByteArray(),
            scoringSpec.toByteArray(),
            resultSpec.toByteArray());
    if (searchResultBytes == null) {
      Log.e(TAG, "Received null SearchResultProto from native.");
      return SearchResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return SearchResultProto.parseFrom(searchResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing SearchResultProto.", e);
      return SearchResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public DeleteResultProto delete(@NonNull String namespace, @NonNull String uri) {
    byte[] deleteResultBytes = nativeDelete(nativePointer, namespace, uri);
    if (deleteResultBytes == null) {
      Log.e(TAG, "Received null DeleteResultProto from native.");
      return DeleteResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return DeleteResultProto.parseFrom(deleteResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing DeleteResultProto.", e);
      return DeleteResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public DeleteByNamespaceResultProto deleteByNamespace(@NonNull String namespace) {
    byte[] deleteByNamespaceResultBytes = nativeDeleteByNamespace(nativePointer, namespace);
    if (deleteByNamespaceResultBytes == null) {
      Log.e(TAG, "Received null DeleteByNamespaceResultProto from native.");
      return DeleteByNamespaceResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return DeleteByNamespaceResultProto.parseFrom(
          deleteByNamespaceResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing DeleteByNamespaceResultProto.", e);
      return DeleteByNamespaceResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public DeleteBySchemaTypeResultProto deleteBySchemaType(@NonNull String schemaType) {
    byte[] deleteBySchemaTypeResultBytes = nativeDeleteBySchemaType(nativePointer, schemaType);
    if (deleteBySchemaTypeResultBytes == null) {
      Log.e(TAG, "Received null DeleteBySchemaTypeResultProto from native.");
      return DeleteBySchemaTypeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return DeleteBySchemaTypeResultProto.parseFrom(
          deleteBySchemaTypeResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing DeleteBySchemaTypeResultProto.", e);
      return DeleteBySchemaTypeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public PersistToDiskResultProto persistToDisk() {
    byte[] persistToDiskResultBytes = nativePersistToDisk(nativePointer);
    if (persistToDiskResultBytes == null) {
      Log.e(TAG, "Received null PersistToDiskResultProto from native.");
      return PersistToDiskResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return PersistToDiskResultProto.parseFrom(persistToDiskResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing PersistToDiskResultProto.", e);
      return PersistToDiskResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public OptimizeResultProto optimize() {
    byte[] optimizeResultBytes = nativeOptimize(nativePointer);
    if (optimizeResultBytes == null) {
      Log.e(TAG, "Received null OptimizeResultProto from native.");
      return OptimizeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return OptimizeResultProto.parseFrom(optimizeResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing OptimizeResultProto.", e);
      return OptimizeResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public GetOptimizeInfoResultProto getOptimizeInfo() {
    byte[] getOptimizeInfoResultBytes = nativeGetOptimizeInfo(nativePointer);
    if (getOptimizeInfoResultBytes == null) {
      Log.e(TAG, "Received null GetOptimizeInfoResultProto from native.");
      return GetOptimizeInfoResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return GetOptimizeInfoResultProto.parseFrom(
          getOptimizeInfoResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing GetOptimizeInfoResultProto.", e);
      return GetOptimizeInfoResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  @NonNull
  public ResetResultProto reset() {
    byte[] resetResultBytes = nativeReset(nativePointer);
    if (resetResultBytes == null) {
      Log.e(TAG, "Received null ResetResultProto from native.");
      return ResetResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }

    try {
      return ResetResultProto.parseFrom(resetResultBytes, EXTENSION_REGISTRY_LITE);
    } catch (InvalidProtocolBufferException e) {
      Log.e(TAG, "Error parsing ResetResultProto.", e);
      return ResetResultProto.newBuilder()
          .setStatus(StatusProto.newBuilder().setCode(StatusProto.Code.INTERNAL))
          .build();
    }
  }

  private static native long nativeCreate(byte[] icingSearchEngineOptionsBytes);

  private static native byte[] nativeInitialize(long nativePointer);

  private static native byte[] nativeSetSchema(
      long nativePointer, byte[] schemaBytes, boolean ignoreErrorsAndDeleteDocuments);

  private static native byte[] nativeGetSchema(long nativePointer);

  private static native byte[] nativeGetSchemaType(long nativePointer, String schemaType);

  private static native byte[] nativePut(long nativePointer, byte[] documentBytes);

  private static native byte[] nativeGet(long nativePointer, String namespace, String uri);

  private static native byte[] nativeGetAllNamespaces(long nativePointer);

  private static native byte[] nativeSearch(
      long nativePointer, byte[] searchSpecBytes, byte[] scoringSpecBytes, byte[] resultSpecBytes);

  private static native byte[] nativeDelete(long nativePointer, String namespace, String uri);

  private static native byte[] nativeDeleteByNamespace(long nativePointer, String namespace);

  private static native byte[] nativeDeleteBySchemaType(long nativePointer, String schemaType);

  private static native byte[] nativePersistToDisk(long nativePointer);

  private static native byte[] nativeOptimize(long nativePointer);

  private static native byte[] nativeGetOptimizeInfo(long nativePointer);

  private static native byte[] nativeReset(long nativePointer);
}
