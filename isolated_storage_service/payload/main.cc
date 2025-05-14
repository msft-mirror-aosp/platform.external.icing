#include <android/binder_auto_utils.h>
#include <android/binder_ibinder.h>
#include <android/binder_status.h>
#include <vm_payload.h>

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <dlfcn.h>
#include <fstream>
#include <memory>
#include <unistd.h>

#include "aidl/com/android/isolated_storage_service/BnIcingSearchEngine.h"
#include "aidl/com/android/isolated_storage_service/BnIsolatedStorageService.h"
#include "icing/icing-search-engine.h"
#include "icing/proto/blob.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/util/logging.h"
#include "macros.h"

namespace {

using ::aidl::com::android::isolated_storage_service::BnIcingSearchEngine;
using ::aidl::com::android::isolated_storage_service::BnIsolatedStorageService;
using ::icing::lib::BatchGetResultProto;
using ::icing::lib::BatchPutResultProto;
using ::icing::lib::BlobProto;
using ::icing::lib::DebugInfoResultProto;
using ::icing::lib::DebugInfoVerbosity;
using ::icing::lib::DeleteByNamespaceResultProto;
using ::icing::lib::DeleteByQueryResultProto;
using ::icing::lib::DeleteBySchemaTypeResultProto;
using ::icing::lib::DeleteResultProto;
using ::icing::lib::DocumentProto;
using ::icing::lib::GetAllNamespacesResultProto;
using ::icing::lib::GetOptimizeInfoResultProto;
using ::icing::lib::GetResultProto;
using ::icing::lib::GetResultSpecProto;
using ::icing::lib::GetSchemaResultProto;
using ::icing::lib::GetSchemaTypeResultProto;
using ::icing::lib::IcingSearchEngine;
using ::icing::lib::IcingSearchEngineOptions;
using ::icing::lib::InitializeResultProto;
using ::icing::lib::OptimizeResultProto;
using ::icing::lib::PersistToDiskResultProto;
using ::icing::lib::PersistType;
using ::icing::lib::PutDocumentRequest;
using ::icing::lib::PutResultProto;
using ::icing::lib::ReportUsageResultProto;
using ::icing::lib::ResetResultProto;
using ::icing::lib::ResultSpecProto;
using ::icing::lib::SchemaProto;
using ::icing::lib::ScoringSpecProto;
using ::icing::lib::SearchResultProto;
using ::icing::lib::SearchSpecProto;
using ::icing::lib::SetSchemaRequestProto;
using ::icing::lib::SetSchemaResultProto;
using ::icing::lib::StatusProto;
using ::icing::lib::StorageInfoResultProto;
using ::icing::lib::SuggestionResponse;
using ::icing::lib::SuggestionSpecProto;
using ::icing::lib::TermMatchType;
using ::icing::lib::UsageReport;
using BlobHandleProto = ::icing::lib::PropertyProto::BlobHandleProto;
using ::icing::lib::ERROR;
using ::icing::lib::INFO;
using ::ndk::ScopedAStatus;

namespace {
void vmShrinkRay() { sync(); }
}  // namespace

// TODO(b/413761935) move better or equivalent solution into AVF
// TODO - is there a way to make dlopen automatically fill in weak symbols?
// please tell smoreland@
struct AVmPayloadLazy {
    decltype(AVmPayload_getEncryptedStoragePath)* AVmPayload_getEncryptedStoragePath = nullptr;
    decltype(AVmPayload_notifyPayloadReady)* AVmPayload_notifyPayloadReady = nullptr;
    decltype(AVmPayload_runVsockRpcServer)* AVmPayload_runVsockRpcServer = nullptr;

    void load() {
      void* libvmpayload = dlopen("libvm_payload.so", RTLD_NOW | RTLD_GLOBAL);
      if (libvmpayload == nullptr) {
        ICING_LOG(ERROR) << "Failed to load libvm_payload.so: " << dlerror();
        abort();
      }
#define LOAD_ONE(sym) do { \
    sym = (decltype(sym)) dlsym(libvmpayload, #sym); \
    if (sym == nullptr) { ICING_LOG(ERROR) << "Failed to load " #sym << dlerror(); } \
    } while(false)

      LOAD_ONE(AVmPayload_getEncryptedStoragePath);
      LOAD_ONE(AVmPayload_notifyPayloadReady);
      LOAD_ONE(AVmPayload_runVsockRpcServer);

#undef LOAD_ONE
    }
} gVmPayloadLazy;

// This class implements the AIDL interface for the Icing connection.
class IcingConnectionImpl
    : public aidl::com::android::isolated_storage_service::BnIcingSearchEngine {
 public:
  explicit IcingConnectionImpl(uint32_t user_id) : user_id_(user_id) {}

  ScopedAStatus initialize(
      const std::vector<uint8_t>& icing_search_engine_options_proto,
      std::optional<std::vector<uint8_t>>* initialize_result_proto) {
    if (icing_ == nullptr) {
      // Only create a new IcingSearchEngine instance if it is nullptr. This
      // will avoid unnecessary object destruction and instantiation if this API
      // is called more than one time.
      IcingSearchEngineOptions options;
      DESERIALIZE_OR_RETURN(icing_search_engine_options_proto, options);
      options.set_base_dir(std::string(gVmPayloadLazy.AVmPayload_getEncryptedStoragePath()) +
                           "/" + std::to_string(user_id_) + "/" +
                           options.base_dir());
      icing_ = std::make_unique<IcingSearchEngine>(options);
    }

    // IcingSearchEngine::Initialize will return success directly if it has
    // already been initialized.
    InitializeResultProto initialize_result = icing_->Initialize();
    SERIALIZE_AND_RETURN_ASTATUS(initialize_result, initialize_result_proto);
  }

  ScopedAStatus close() {
    CHECK_ICING_INIT(icing_);
    ICING_LOG(INFO) << "IsolatedStorageService closing Icing connection.";
    icing_->PersistToDisk(icing::lib::PersistType::FULL);
    return ScopedAStatus::ok();
  }

  ScopedAStatus clearAndDestroy(
      std::optional<std::vector<uint8_t>>* clear_and_destroy_result_proto) {
    CHECK_ICING_INIT(icing_);
    ICING_LOG(INFO)
        << "IsolatedStorageService clear and destroy icing instance.";
    ResetResultProto clear_and_destroy_result = icing_->ClearAndDestroy();
    SERIALIZE_AND_RETURN_ASTATUS(clear_and_destroy_result,
                                 clear_and_destroy_result_proto);
  }

  ScopedAStatus reset(std::optional<std::vector<uint8_t>>* reset_result_proto) {
    CHECK_ICING_INIT(icing_);
    ResetResultProto reset_result = icing_->Reset();
    SERIALIZE_AND_RETURN_ASTATUS(reset_result, reset_result_proto);
  }

  ScopedAStatus setSchema(
      const std::vector<uint8_t>& schema_proto,
      bool ignore_errors_and_delete_documents,
      std::optional<std::vector<uint8_t>>* set_schema_result_proto) {
    CHECK_ICING_INIT(icing_);

    SchemaProto schema;
    DESERIALIZE_OR_RETURN(schema_proto, schema)

    SetSchemaResultProto set_schema_result =
        icing_->SetSchema(schema, ignore_errors_and_delete_documents);
    SERIALIZE_AND_RETURN_ASTATUS(set_schema_result, set_schema_result_proto);
  }

  ScopedAStatus setSchemaWithRequestProto(
      const std::vector<uint8_t>& set_schema_request_proto,
      std::optional<std::vector<uint8_t>>* set_schema_result_proto) {
    CHECK_ICING_INIT(icing_);

    SetSchemaRequestProto request;
    DESERIALIZE_OR_RETURN(set_schema_request_proto, request)

    SetSchemaResultProto set_schema_result = icing_->SetSchema(std::move(request));
    SERIALIZE_AND_RETURN_ASTATUS(set_schema_result, set_schema_result_proto);
  }

  ScopedAStatus getSchema(
      std::optional<std::vector<uint8_t>>* get_schema_result_proto) {
    CHECK_ICING_INIT(icing_);

    GetSchemaResultProto schema = icing_->GetSchema();
    SERIALIZE_AND_RETURN_ASTATUS(schema, get_schema_result_proto);
  }

  ScopedAStatus getSchemaForDatabase(
      const std::string& database,
      std::optional<std::vector<uint8_t>>* get_schema_result_proto) {
    CHECK_ICING_INIT(icing_);
    GetSchemaResultProto schema = icing_->GetSchema(database);
    SERIALIZE_AND_RETURN_ASTATUS(schema, get_schema_result_proto);
  }

  ScopedAStatus getSchemaType(
      const std::string& schema_type,
      std::optional<std::vector<uint8_t>>* get_schema_type_result_proto) {
    CHECK_ICING_INIT(icing_);

    GetSchemaTypeResultProto schema_type_result =
        icing_->GetSchemaType(schema_type);
    SERIALIZE_AND_RETURN_ASTATUS(schema_type_result,
                                 get_schema_type_result_proto);
  }

  ScopedAStatus put(const std::vector<uint8_t>& document_proto,
                    std::optional<std::vector<uint8_t>>* put_result_proto) {
    CHECK_ICING_INIT(icing_);

    DocumentProto document;
    DESERIALIZE_OR_RETURN(document_proto, document);
    PutResultProto put_result = icing_->Put(document);
    *put_result_proto = std::vector<uint8_t>();
    SERIALIZE_AND_RETURN_ASTATUS(put_result, put_result_proto);
  }

  ScopedAStatus batchPut(
      const std::vector<uint8_t>& put_document_request_proto,
      std::optional<std::vector<uint8_t>>* batch_put_result_proto) {
    CHECK_ICING_INIT(icing_);

    PutDocumentRequest request;
    DESERIALIZE_OR_RETURN(put_document_request_proto, request);

    BatchPutResultProto result = icing_->BatchPut(std::move(request));

    *batch_put_result_proto = std::vector<uint8_t>();
    SERIALIZE_AND_RETURN_ASTATUS(result, batch_put_result_proto);
  }

  ScopedAStatus get(const std::string& name_space, const std::string& uri,
                    const std::vector<uint8_t>& get_result_spec_proto,
                    std::optional<std::vector<uint8_t>>* get_result_proto) {
    CHECK_ICING_INIT(icing_);

    GetResultSpecProto get_result_spec;
    DESERIALIZE_OR_RETURN(get_result_spec_proto, get_result_spec);

    GetResultProto get_result = icing_->Get(name_space, uri, get_result_spec);
    SERIALIZE_AND_RETURN_ASTATUS(get_result, get_result_proto);
  }

  ScopedAStatus batchGet(
      const std::vector<uint8_t>& get_result_spec_proto,
      std::optional<std::vector<uint8_t>>* batch_get_result_proto) {
    CHECK_ICING_INIT(icing_);

    GetResultSpecProto get_result_spec;
    DESERIALIZE_OR_RETURN(get_result_spec_proto, get_result_spec);

    BatchGetResultProto batch_get_result =
        icing_->BatchGet(std::move(get_result_spec));
    SERIALIZE_AND_RETURN_ASTATUS(batch_get_result, batch_get_result_proto);
  }

  ScopedAStatus reportUsage(
      const std::vector<uint8_t>& usage_report_proto,
      std::optional<std::vector<uint8_t>>* report_usage_result_proto) {
    CHECK_ICING_INIT(icing_);

    UsageReport usage_report;
    DESERIALIZE_OR_RETURN(usage_report_proto, usage_report);

    ReportUsageResultProto report_usage_result =
        icing_->ReportUsage(usage_report);
    SERIALIZE_AND_RETURN_ASTATUS(report_usage_result,
                                 report_usage_result_proto);
  }

  ScopedAStatus getAllNamespaces(
      std::optional<std::vector<uint8_t>>* get_all_namespaces_result_proto) {
    CHECK_ICING_INIT(icing_);

    GetAllNamespacesResultProto get_all_namespaces_result =
        icing_->GetAllNamespaces();
    SERIALIZE_AND_RETURN_ASTATUS(get_all_namespaces_result,
                                 get_all_namespaces_result_proto);
  }

  ScopedAStatus search(
      const std::vector<uint8_t>& search_spec_proto,
      const std::vector<uint8_t>& scoring_spec_proto,
      const std::vector<uint8_t>& result_spec_proto,
      std::optional<std::vector<uint8_t>>* search_result_proto) {
    CHECK_ICING_INIT(icing_);

    SearchSpecProto search_spec;
    DESERIALIZE_OR_RETURN(search_spec_proto, search_spec);
    ScoringSpecProto scoring_spec;
    DESERIALIZE_OR_RETURN(scoring_spec_proto, scoring_spec);
    ResultSpecProto result_spec;
    DESERIALIZE_OR_RETURN(result_spec_proto, result_spec);

    SearchResultProto search_result =
        icing_->Search(search_spec, scoring_spec, result_spec);
    SERIALIZE_AND_RETURN_ASTATUS(search_result, search_result_proto);

    return ScopedAStatus::ok();
  }

  ScopedAStatus getNextPage(
      int64_t next_page_token,
      std::optional<std::vector<uint8_t>>* get_next_page_result_proto) {
    CHECK_ICING_INIT(icing_);

    SearchResultProto get_next_page_result =
        icing_->GetNextPage(next_page_token);
    SERIALIZE_AND_RETURN_ASTATUS(get_next_page_result,
                                 get_next_page_result_proto);
  }

  ScopedAStatus invalidateNextPageToken(int64_t next_page_token) {
    CHECK_ICING_INIT(icing_);

    icing_->InvalidateNextPageToken(next_page_token);
    return ScopedAStatus::ok();
  }

  ScopedAStatus openWriteBlob(const std::vector<uint8_t>& blob_handle_proto,
                              std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto open_write_blob_result = icing_->OpenWriteBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(open_write_blob_result, blob_proto);
  }

  ScopedAStatus removeBlob(const std::vector<uint8_t>& blob_handle_proto,
                           std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto remove_blob_result = icing_->RemoveBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(remove_blob_result, blob_proto);
  }

  ScopedAStatus openReadBlob(const std::vector<uint8_t>& blob_handle_proto,
                             std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto open_read_blob_result = icing_->OpenReadBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(open_read_blob_result, blob_proto);
  }

  ScopedAStatus commitBlob(const std::vector<uint8_t>& blob_handle_proto,
                           std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto commit_blob_result = icing_->CommitBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(commit_blob_result, blob_proto);
  }

  ScopedAStatus deleteDoc(
      const std::string& name_space, const std::string& uri,
      std::optional<std::vector<uint8_t>>* delete_result_proto) {
    CHECK_ICING_INIT(icing_);

    DeleteResultProto delete_result = icing_->Delete(name_space, uri);
    SERIALIZE_AND_RETURN_ASTATUS(delete_result, delete_result_proto);
  }

  ScopedAStatus searchSuggestions(
      const std::vector<uint8_t>& suggestion_spec_proto,
      std::optional<std::vector<uint8_t>>* suggestion_response_proto) {
    CHECK_ICING_INIT(icing_);

    SuggestionSpecProto suggestion_spec;
    DESERIALIZE_OR_RETURN(suggestion_spec_proto, suggestion_spec);

    SuggestionResponse suggestion_response =
        icing_->SearchSuggestions(suggestion_spec);
    SERIALIZE_AND_RETURN_ASTATUS(suggestion_response,
                                 suggestion_response_proto);
  }

  ScopedAStatus deleteByNamespace(
      const std::string& name_space,
      std::optional<std::vector<uint8_t>>* delete_by_namespace_result_proto) {
    CHECK_ICING_INIT(icing_);

    DeleteByNamespaceResultProto delete_by_namespace_result =
        icing_->DeleteByNamespace(name_space);
    SERIALIZE_AND_RETURN_ASTATUS(delete_by_namespace_result,
                                 delete_by_namespace_result_proto);
  }

  ScopedAStatus deleteBySchemaType(
      const std::string& schema_type,
      std::optional<std::vector<uint8_t>>* delete_by_schema_type_result_proto) {
    CHECK_ICING_INIT(icing_);

    DeleteBySchemaTypeResultProto delete_by_schema_type_result =
        icing_->DeleteBySchemaType(schema_type);
    SERIALIZE_AND_RETURN_ASTATUS(delete_by_schema_type_result,
                                 delete_by_schema_type_result_proto);
  }

  ScopedAStatus deleteByQuery(
      const std::vector<uint8_t>& search_spec_proto,
      bool return_deleted_document_info,
      std::optional<std::vector<uint8_t>>* delete_by_query_result_proto) {
    CHECK_ICING_INIT(icing_);

    SearchSpecProto search_spec;
    DESERIALIZE_OR_RETURN(search_spec_proto, search_spec);

    DeleteByQueryResultProto delete_by_query_result =
        icing_->DeleteByQuery(search_spec, return_deleted_document_info);
    SERIALIZE_AND_RETURN_ASTATUS(delete_by_query_result,
                                 delete_by_query_result_proto);
  }

  ScopedAStatus persistToDisk(
      int32_t persist_type_code,
      std::optional<std::vector<uint8_t>>* persist_to_disk_result_proto) {
    CHECK_ICING_INIT(icing_);

    PersistToDiskResultProto persist_to_disk_result =
        icing_->PersistToDisk(PersistType::Code(persist_type_code));
    SERIALIZE_AND_RETURN_ASTATUS(persist_to_disk_result,
                                 persist_to_disk_result_proto);
  }

  ScopedAStatus optimize(
      std::optional<std::vector<uint8_t>>* optimize_result_proto) {
    CHECK_ICING_INIT(icing_);

    OptimizeResultProto optimize_result = icing_->Optimize();
    SERIALIZE_AND_RETURN_ASTATUS(optimize_result, optimize_result_proto);
  }

  ScopedAStatus getOptimizeInfo(
      std::optional<std::vector<uint8_t>>* get_optimize_info_result_proto) {
    CHECK_ICING_INIT(icing_);

    GetOptimizeInfoResultProto get_optimize_info_result =
        icing_->GetOptimizeInfo();
    SERIALIZE_AND_RETURN_ASTATUS(get_optimize_info_result,
                                 get_optimize_info_result_proto);
  }

  ScopedAStatus getStorageInfo(
      std::optional<std::vector<uint8_t>>* get_storage_info_result_proto) {
    CHECK_ICING_INIT(icing_);

    StorageInfoResultProto get_storage_info_result = icing_->GetStorageInfo();
    SERIALIZE_AND_RETURN_ASTATUS(get_storage_info_result,
                                 get_storage_info_result_proto);
  }

  ScopedAStatus getDebugInfo(
      int32_t verbosity,
      std::optional<std::vector<uint8_t>>* get_debug_info_result_proto) {
    CHECK_ICING_INIT(icing_);

    DebugInfoResultProto get_debug_info_result =
        icing_->GetDebugInfo(DebugInfoVerbosity::Code(verbosity));
    SERIALIZE_AND_RETURN_ASTATUS(get_debug_info_result,
                                 get_debug_info_result_proto);
  }

 protected:
  std::unique_ptr<icing::lib::IcingSearchEngine> icing_ = nullptr;
  uint32_t user_id_;
};

class IsolatedStorageServiceImpl : public BnIsolatedStorageService {
 public:
  IsolatedStorageServiceImpl() = default;

 private:
  ScopedAStatus quit() override {
    ICING_LOG(INFO) << "Received quit request, exiting";
    for (const auto& [unused, connection] : icing_connections_) {
      connection->close();
    }
    exit(0);
  }

  ScopedAStatus trimMemory() override {
    ICING_LOG(INFO) << "Received trim memory request, trimming";
    vmShrinkRay();
    return ScopedAStatus::ok();
  }

  ScopedAStatus getAvailableMemory(int64_t* mem_available) override {
    std::ifstream meminfo_file("/proc/meminfo");
    if (!meminfo_file) {
      return ScopedAStatus::fromExceptionCodeWithMessage(
          EX_ILLEGAL_STATE, "Failed to open /proc/meminfo");
    }
    constexpr std::string_view kMemAvailableStr = "MemAvailable:";
    std::string line;
    while (std::getline(meminfo_file, line)) {
      if (line.starts_with(kMemAvailableStr)) {
        // It is possible that "kB" is in the end of the line, so let's use
        // sscanf to parse int64_t.
        int64_t temp_val = 0;
        if (sscanf(line.c_str() + kMemAvailableStr.size(), "%" PRId64, &temp_val)
            != 1) {
          // Failed to parse int64_t. This should not happen, but let's handle
          // it just in case.
          return ScopedAStatus::fromExceptionCodeWithMessage(
              EX_ILLEGAL_STATE, "Failed to parse MemAvailable");
        }

        *mem_available = temp_val;
        return ScopedAStatus::ok();
      }
    }
    return ScopedAStatus::fromExceptionCodeWithMessage(
        EX_ILLEGAL_STATE, "Failed to find MemAvailable");
  }

  ScopedAStatus getOrCreateIcingConnection(
      int32_t user_id,
      std::shared_ptr<
          aidl::com::android::isolated_storage_service::IIcingSearchEngine>*
          icing_server) override {
    auto connection = icing_connections_.find(user_id);
    if (connection != icing_connections_.end()) {
      *icing_server = connection->second;
      return ScopedAStatus::ok();
    }
    icing_connections_[user_id] =
        ndk::SharedRefBase::make<IcingConnectionImpl>(user_id);
    *icing_server = icing_connections_[user_id];
    return ScopedAStatus::ok();
  }

  ScopedAStatus removeIcingConnection(int user_id) override {
    ICING_LOG(INFO) << "Removing Icing connection for user " << user_id;
    auto connection = icing_connections_.find(user_id);
    if (connection != icing_connections_.end()) {
      icing_connections_.erase(connection);
    }
    return ScopedAStatus::ok();
  }

  std::map<int32_t, std::shared_ptr<IcingConnectionImpl>> icing_connections_;
};
}  // namespace

extern "C" int AVmPayload_main() {
  gVmPayloadLazy.load();

  // TODO(b/401363381): Remove this once we have a better way to log to
  // /dev/hvc2 in isolated storage.
  // Force logging to /dev/hvc2 in isolated storage.
  icing::lib::SetForceDebugLogging(true);
  ICING_LOG(INFO) << "IsolatedStorageService VM Payload starting";
  auto service = ndk::SharedRefBase::make<IsolatedStorageServiceImpl>();
  auto callback = []([[maybe_unused]] void* param) {
    ICING_LOG(INFO) << "IsolatedStorageService VM Payload ready";
    gVmPayloadLazy.AVmPayload_notifyPayloadReady();
  };
  gVmPayloadLazy.AVmPayload_runVsockRpcServer(service->asBinder().get(), service->PORT,
                               callback, /*param=*/nullptr);
}
