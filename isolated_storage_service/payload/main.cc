#include <android-base/properties.h>
#include <android/binder_auto_utils.h>
#include <android/binder_ibinder.h>
#include <android/binder_status.h>
#include <signal.h>
#include <sys/system_properties.h>
#include <vm_payload.h>

#include <chrono>
#include <cinttypes>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <dlfcn.h>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

#include "aidl/com/android/isolated_storage_service/BnIcingSearchEngine.h"
#include "aidl/com/android/isolated_storage_service/BnIsolatedStorageService.h"
#include "icing/absl_ports/thread_annotations.h"
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

constexpr std::string_view ENCRYPTED_STORE_SETUP_PROP = "microdroid_manager.encrypted_store.setup";
constexpr std::string_view ENCRYPTED_STORE_STATUS_PROP = "microdroid_manager.encrypted_store.status";

using android::base::WaitForProperty;

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
using ::icing::lib::GetNextPageRequestProto;
using ::icing::lib::GetOptimizeInfoResultProto;
using ::icing::lib::GetResultProto;
using ::icing::lib::GetResultSpecProto;
using ::icing::lib::GetSchemaResultProto;
using ::icing::lib::GetSchemaTypeResultProto;
using ::icing::lib::HandleExpiredDocumentsResultProto;
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
using ::icing::lib::WARNING;
using ::ndk::ScopedAStatus;

namespace {
void vmShrinkRay() { sync(); }
}  // namespace

enum class ServiceState {
  kAlive,
  kTerminate,
};

// A wrapper struct for state information of a single IcingConnectionImpl to
// handle termination.
struct ConnectionState {
  std::mutex mutex;
  std::condition_variable cv;

  ServiceState state ICING_GUARDED_BY(mutex);
  int active_request_count ICING_GUARDED_BY(mutex);

  class ActiveRequest {
   public:
    explicit ActiveRequest(ConnectionState* conn_state)
        : conn_state_(conn_state) {}

    ~ActiveRequest() {
      conn_state_->DestroyActiveRequest();
    }

   private:
    ConnectionState* conn_state_;  // Does not own!
  };

  ConnectionState() : state(ServiceState::kAlive), active_request_count(0) {}

  // Returns an ActiveRequest that handles active request count increment and
  // decrement.
  //
  // Note: nullopt will be returned if the service is about to terminate, and the
  //   request should be rejected.
  std::optional<ActiveRequest> CreateActiveRequest() {
    std::unique_lock lk(mutex);

    if (state == ServiceState::kTerminate) {
      return std::nullopt;
    }

    ++active_request_count;
    return std::make_optional<ActiveRequest>(this);
  }

  void DestroyActiveRequest() {
    bool need_notify = false;
    {
      std::unique_lock lk(mutex);

      --active_request_count;
      if (state == ServiceState::kTerminate) {
        ICING_LOG(INFO) << "Finish request at terminate state. "
                        << active_request_count
                        << " active request(s) remaining.";
        // If we're at terminate state and this is the last active request, then
        // notify the waiting thread and run the cleanup tasks.
        if (active_request_count == 0) {
          need_notify = true;
        }
      }
    }

    if (need_notify) {
      ICING_LOG(INFO) << "Notify the main thread to cleanup before termination.";
      cv.notify_all();
    }
  }
};

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

#define CREATE_ACTIVE_REQUEST_AND_CHECK(expr)                  \
  auto active_request = expr;                                  \
  do {                                                         \
    if (active_request == std::nullopt) {                      \
      return ndk::ScopedAStatus::fromExceptionCodeWithMessage( \
          EX_ILLEGAL_STATE, "Service is about to terminate");  \
    }                                                          \
  } while (false)

// This class implements the AIDL interface for the Icing connection.
class IcingConnectionImpl
    : public aidl::com::android::isolated_storage_service::BnIcingSearchEngine {
 public:
  explicit IcingConnectionImpl(uint32_t user_id) : user_id_(user_id) {}

  void Terminate() {
    // Set terminate state and wait for active requests to finish.
    {
      ICING_LOG(INFO) << "Acquiring the lock";
      std::unique_lock lk(conn_state_.mutex);

      conn_state_.state = ServiceState::kTerminate;
      if (conn_state_.active_request_count > 0) {
        ICING_LOG(INFO) << "Wait for " << conn_state_.active_request_count
                        << " active requests for user " << user_id_
                        << " to finish";

        // Conditional variable predicate: end waiting when there is no active
        // request.
        const auto pred = [&]() {
          return conn_state_.active_request_count <= 0;
        };
        while (!conn_state_.cv.wait_for(lk, std::chrono::seconds(1), pred)) {
          ICING_LOG(INFO) << "Waited 1s for active requests to finish,"
                          << " but there are still "
                          << conn_state_.active_request_count
                          << " active requests left.";
        }

        ICING_LOG(INFO) << "Got notification from the last active request for user "
                        << user_id_;
      } else {
        ICING_LOG(INFO) << "No active requests for user " << user_id_;
      }
    }

    // At this point, we've:
    // - Set state to kTerminate.
    // - Ensured there is no active request.
    //
    // Call PersistToDisk to cleanup. RECOVERY_PROOF mode is sufficient here
    // since it updates all essential checksums. Data flushing will be handled
    // by the OS shutdown path.
    ICING_LOG(INFO) << "Icing PersistToDisk: safe termination for user "
                    << user_id_;
    PersistToDiskResultProto persist_to_disk_result =
        icing_->PersistToDisk(icing::lib::PersistType::RECOVERY_PROOF);
    if (persist_to_disk_result.status().code() != StatusProto::OK) {
      ICING_LOG(WARNING) << "Failed to handle Icing PersistToDisk for user "
                         << user_id_ << ". Code: "
                         << static_cast<int>(persist_to_disk_result.status().code())
                         << ", message: "
                         << persist_to_disk_result.status().message();
    }
  }

  ScopedAStatus initialize(
      const std::vector<uint8_t>& icing_search_engine_options_proto,
      std::optional<std::vector<uint8_t>>* initialize_result_proto) {
    if (icing_ == nullptr) {
      // Only create a new IcingSearchEngine instance if it is nullptr. This
      // will avoid unnecessary object destruction and instantiation if this API
      // is called more than one time.
      IcingSearchEngineOptions options;
      DESERIALIZE_OR_RETURN(icing_search_engine_options_proto, options);

      // Need to sanitize provided base directory. Valid filenames should only
      // contain letters and numbers. Reject any provided base directories that
      // do not meet this criteria.
      const std::regex pattern("^[a-zA-Z0-9]+$");
      if (options.base_dir().empty() || !std::regex_match(options.base_dir(), pattern)) {
        // return failed init proto to called
        ICING_LOG(ERROR) << "Invalid base_dir " << options.base_dir();

        InitializeResultProto result;
        StatusProto* result_status = result.mutable_status();
        result_status->set_code(StatusProto::INTERNAL);
        result_status->set_message("Invalid base_dir");
        SERIALIZE_AND_RETURN_ASTATUS(result, initialize_result_proto);
      }

      if (gVmPayloadLazy.AVmPayload_getEncryptedStoragePath() == nullptr) {
        ICING_LOG(ERROR) << "Invalid encrypted storage path";

        InitializeResultProto result;
        StatusProto* result_status = result.mutable_status();
        result_status->set_code(StatusProto::INTERNAL);
        result_status->set_message("Invalid encrypted storage path");
        SERIALIZE_AND_RETURN_ASTATUS(result, initialize_result_proto);
      }

      options.set_base_dir(std::string(gVmPayloadLazy.AVmPayload_getEncryptedStoragePath()) +
                           "/" + std::to_string(user_id_) + "/" + options.base_dir());
      icing_ = std::make_unique<IcingSearchEngine>(options);
    }

    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    // IcingSearchEngine::Initialize will return success directly if it has
    // already been initialized.
    InitializeResultProto initialize_result = icing_->Initialize();
    SERIALIZE_AND_RETURN_ASTATUS(initialize_result, initialize_result_proto);
  }

  ScopedAStatus close() {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    ICING_LOG(INFO) << "IsolatedStorageService closing Icing connection.";
    icing_->PersistToDisk(icing::lib::PersistType::FULL);
    return ScopedAStatus::ok();
  }

  ScopedAStatus clearAndDestroy(
      std::optional<std::vector<uint8_t>>* clear_and_destroy_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    ICING_LOG(INFO)
        << "IsolatedStorageService clear and destroy icing instance.";
    ResetResultProto clear_and_destroy_result = icing_->ClearAndDestroy();
    SERIALIZE_AND_RETURN_ASTATUS(clear_and_destroy_result,
                                 clear_and_destroy_result_proto);
  }

  ScopedAStatus reset(std::optional<std::vector<uint8_t>>* reset_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    ResetResultProto reset_result = icing_->Reset();
    SERIALIZE_AND_RETURN_ASTATUS(reset_result, reset_result_proto);
  }

  ScopedAStatus setSchema(
      const std::vector<uint8_t>& schema_proto,
      bool ignore_errors_and_delete_documents,
      std::optional<std::vector<uint8_t>>* set_schema_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    SetSchemaRequestProto request;
    DESERIALIZE_OR_RETURN(set_schema_request_proto, request)

    SetSchemaResultProto set_schema_result = icing_->SetSchema(std::move(request));
    SERIALIZE_AND_RETURN_ASTATUS(set_schema_result, set_schema_result_proto);
  }

  ScopedAStatus getSchema(
      std::optional<std::vector<uint8_t>>* get_schema_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    GetSchemaResultProto schema = icing_->GetSchema();
    SERIALIZE_AND_RETURN_ASTATUS(schema, get_schema_result_proto);
  }

  ScopedAStatus getSchemaForDatabase(
      const std::string& database,
      std::optional<std::vector<uint8_t>>* get_schema_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    GetSchemaResultProto schema = icing_->GetSchema(database);
    SERIALIZE_AND_RETURN_ASTATUS(schema, get_schema_result_proto);
  }

  ScopedAStatus getSchemaType(
      const std::string& schema_type,
      std::optional<std::vector<uint8_t>>* get_schema_type_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    GetSchemaTypeResultProto schema_type_result =
        icing_->GetSchemaType(schema_type);
    SERIALIZE_AND_RETURN_ASTATUS(schema_type_result,
                                 get_schema_type_result_proto);
  }

  ScopedAStatus put(const std::vector<uint8_t>& document_proto,
                    std::optional<std::vector<uint8_t>>* put_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    GetResultSpecProto get_result_spec;
    DESERIALIZE_OR_RETURN(get_result_spec_proto, get_result_spec);

    GetResultProto get_result = icing_->Get(name_space, uri, get_result_spec);
    SERIALIZE_AND_RETURN_ASTATUS(get_result, get_result_proto);
  }

  ScopedAStatus batchGet(
      const std::vector<uint8_t>& get_result_spec_proto,
      std::optional<std::vector<uint8_t>>* batch_get_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    SearchResultProto get_next_page_result =
        icing_->GetNextPage(next_page_token);
    SERIALIZE_AND_RETURN_ASTATUS(get_next_page_result,
                                 get_next_page_result_proto);
  }

  ScopedAStatus getNextPageWithRequestProto(
      const std::vector<uint8_t>& get_next_page_request_proto,
      std::optional<std::vector<uint8_t>>* get_next_page_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    GetNextPageRequestProto request_proto;
    DESERIALIZE_OR_RETURN(get_next_page_request_proto, request_proto);

    SearchResultProto get_next_page_result = icing_->GetNextPage(std::move(request_proto));
    SERIALIZE_AND_RETURN_ASTATUS(get_next_page_result, get_next_page_result_proto);
  }

  ScopedAStatus invalidateNextPageToken(int64_t next_page_token) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    icing_->InvalidateNextPageToken(next_page_token);
    return ScopedAStatus::ok();
  }

  ScopedAStatus handleExpiredDocuments(
      std::optional<std::vector<uint8_t>>* handle_expired_documents_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    HandleExpiredDocumentsResultProto handle_result = icing_->HandleExpiredDocuments();
    SERIALIZE_AND_RETURN_ASTATUS(handle_result, handle_expired_documents_result_proto);
  }

  ScopedAStatus openWriteBlob(const std::vector<uint8_t>& blob_handle_proto,
                              std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto open_write_blob_result = icing_->OpenWriteBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(open_write_blob_result, blob_proto);
  }

  ScopedAStatus removeBlob(const std::vector<uint8_t>& blob_handle_proto,
                           std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto remove_blob_result = icing_->RemoveBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(remove_blob_result, blob_proto);
  }

  ScopedAStatus openReadBlob(const std::vector<uint8_t>& blob_handle_proto,
                             std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto open_read_blob_result = icing_->OpenReadBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(open_read_blob_result, blob_proto);
  }

  ScopedAStatus commitBlob(const std::vector<uint8_t>& blob_handle_proto,
                           std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    BlobHandleProto blob_handle;
    DESERIALIZE_OR_RETURN(blob_handle_proto, blob_handle);

    BlobProto commit_blob_result = icing_->CommitBlob(blob_handle);
    SERIALIZE_AND_RETURN_ASTATUS(commit_blob_result, blob_proto);
  }

  ScopedAStatus getAllBlobInfos(
      std::optional<std::vector<uint8_t>>* blob_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    BlobProto get_all_blob_infos_result = icing_->GetAllBlobInfos();
    SERIALIZE_AND_RETURN_ASTATUS(get_all_blob_infos_result, blob_proto);
  }

  ScopedAStatus putBlobInfos(
      const std::vector<uint8_t>& blob_info_protos_proto,
      std::optional<std::vector<uint8_t>>* result_blob_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    BlobProto blob_info_protos;
    DESERIALIZE_OR_RETURN(blob_info_protos_proto, blob_info_protos);

    BlobProto result = icing_->PutBlobInfos(blob_info_protos);
    SERIALIZE_AND_RETURN_ASTATUS(result, result_blob_proto);
  }

  ScopedAStatus deleteDoc(
      const std::string& name_space, const std::string& uri,
      std::optional<std::vector<uint8_t>>* delete_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    DeleteResultProto delete_result = icing_->Delete(name_space, uri);
    SERIALIZE_AND_RETURN_ASTATUS(delete_result, delete_result_proto);
  }

  ScopedAStatus searchSuggestions(
      const std::vector<uint8_t>& suggestion_spec_proto,
      std::optional<std::vector<uint8_t>>* suggestion_response_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    DeleteByNamespaceResultProto delete_by_namespace_result =
        icing_->DeleteByNamespace(name_space);
    SERIALIZE_AND_RETURN_ASTATUS(delete_by_namespace_result,
                                 delete_by_namespace_result_proto);
  }

  ScopedAStatus deleteBySchemaType(
      const std::string& schema_type,
      std::optional<std::vector<uint8_t>>* delete_by_schema_type_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

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
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    PersistToDiskResultProto persist_to_disk_result =
        icing_->PersistToDisk(PersistType::Code(persist_type_code));
    SERIALIZE_AND_RETURN_ASTATUS(persist_to_disk_result,
                                 persist_to_disk_result_proto);
  }

  ScopedAStatus optimize(
      std::optional<std::vector<uint8_t>>* optimize_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    OptimizeResultProto optimize_result = icing_->Optimize();
    SERIALIZE_AND_RETURN_ASTATUS(optimize_result, optimize_result_proto);
  }

  ScopedAStatus getOptimizeInfo(
      std::optional<std::vector<uint8_t>>* get_optimize_info_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    GetOptimizeInfoResultProto get_optimize_info_result =
        icing_->GetOptimizeInfo();
    SERIALIZE_AND_RETURN_ASTATUS(get_optimize_info_result,
                                 get_optimize_info_result_proto);
  }

  ScopedAStatus getStorageInfo(
      std::optional<std::vector<uint8_t>>* get_storage_info_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    StorageInfoResultProto get_storage_info_result = icing_->GetStorageInfo();
    SERIALIZE_AND_RETURN_ASTATUS(get_storage_info_result,
                                 get_storage_info_result_proto);
  }

  ScopedAStatus getDebugInfo(
      int32_t verbosity,
      std::optional<std::vector<uint8_t>>* get_debug_info_result_proto) {
    CHECK_ICING_INIT(icing_);
    CREATE_ACTIVE_REQUEST_AND_CHECK(conn_state_.CreateActiveRequest());

    DebugInfoResultProto get_debug_info_result =
        icing_->GetDebugInfo(DebugInfoVerbosity::Code(verbosity));
    SERIALIZE_AND_RETURN_ASTATUS(get_debug_info_result,
                                 get_debug_info_result_proto);
  }

 protected:
  std::unique_ptr<icing::lib::IcingSearchEngine> icing_ = nullptr;
  uint32_t user_id_;

  ConnectionState conn_state_;
};

class IsolatedStorageServiceImpl : public BnIsolatedStorageService {
 public:
  IsolatedStorageServiceImpl() = default;

  // Handle terminate.
  void Terminate() {
    std::map<int32_t, std::shared_ptr<IcingConnectionImpl>> connections;
    // Step 1: set the service state to terminate to reject any new connections.
    //   Also move out all existing connections so we can release the lock and
    //   handle termination outside of the critical section.
    {
      std::unique_lock lk(mutex_);

      state_ = ServiceState::kTerminate;
      connections.swap(icing_connections_);
    }

    // Step 2: terminate each connection.
    for (const auto& [unused, connection] : connections) {
      connection->Terminate();
    }

    // Step 3: goodbye.
    ICING_LOG(INFO) << "Done cleanup connections. Terminate payload service.";
    exit(0);
  }

 private:
  ScopedAStatus quit() override {
    ICING_LOG(INFO) << "Received quit request, exiting";
    for (const auto& [unused, connection] : icing_connections_) {
      connection->close();
    }
    exit(0);
  }

  // Initially, when microdroid starts, the CE key associated with
  // /mnt/encryptedstore is unavailable, and hence the filesystem
  // cannot be decrypted. The onUserUnlocking call is used to notify
  // microdroid that the CE directory is mounted, and hence the
  // CE key can be retrieved by the VM.
  //
  // For more details, please see commit
  // 7cc0c9b6f1102ac4e53ef63ae4731f0d1811826d in
  // packages/modules/Virtualization
  //
  // This code has two steps:
  //
  // 1) Signal microdroid that the user has unlocked their device.
  // 2) Wait until microdroid has completed the encrypted filesystem setup
  //
  ScopedAStatus onUserUnlocking() override {
    ICING_LOG(INFO) << "onUserUnlocking";

    // Signal microdroid that the CE directory is available by setting
    // the property ENCRYPTED_STORE_SETUP_PROP

    if (__system_property_set(std::string(ENCRYPTED_STORE_SETUP_PROP).c_str(), "true") != 0) {
      std::string error = "failed to set property " + std::string(ENCRYPTED_STORE_SETUP_PROP);
      ICING_LOG(ERROR) << error;
      return ScopedAStatus::fromExceptionCodeWithMessage(EX_SERVICE_SPECIFIC, error.c_str());
    }

    // microdroid_manager uses getEncryptedStoreKEK API on
    // IVirtualMachineService to get an IEncryptedStoreKEK object stored
    // inside app's private directory on the Android host.
    //
    // Wait for that operation to complete by waiting on the
    // ENCRYPTED_STORE_STATUS_PROP property.

    if (!WaitForProperty(std::string(ENCRYPTED_STORE_STATUS_PROP), "ready")) {
        return ScopedAStatus::fromExceptionCodeWithMessage(
            EX_SERVICE_SPECIFIC, "encrypted store not available");
    }

    return ScopedAStatus::ok();
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
    std::unique_lock lk(mutex_);

    if (state_ == ServiceState::kTerminate) {
      return ScopedAStatus::fromExceptionCodeWithMessage(
          EX_ILLEGAL_STATE, "Service is about to terminate");
    }

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
    std::unique_lock lk(mutex_);

    ICING_LOG(INFO) << "Removing Icing connection for user " << user_id;
    auto connection = icing_connections_.find(user_id);
    if (connection != icing_connections_.end()) {
      icing_connections_.erase(connection);
    }
    return ScopedAStatus::ok();
  }

  std::mutex mutex_;
  ServiceState state_ ICING_GUARDED_BY(mutex_);
  std::map<int32_t, std::shared_ptr<IcingConnectionImpl>>
      icing_connections_ ICING_GUARDED_BY(mutex_);
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

  // Create a new thread to wait for SIGTERM/SIGINT and shutdown gracefully.
  // The signals must be blocked in the main thread (and all other threads) so
  // that they are not delivered using the default handlers.
  sigset_t sig_set;
  sigemptyset(&sig_set);
  sigaddset(&sig_set, SIGINT);
  sigaddset(&sig_set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &sig_set, nullptr);

  std::thread([service, sig_set] {
    int sig;
    int ret = sigwait(&sig_set, &sig);
    if (ret == 0) {
      ICING_LOG(INFO) << "Caught signal " << sig << ", shutting down.";
      service->Terminate();
    } else {
      ICING_LOG(ERROR) << "sigwait failed: " << strerror(ret);
    }
  }).detach();

  auto callback = []([[maybe_unused]] void* param) {
    ICING_LOG(INFO) << "IsolatedStorageService VM Payload ready";
    gVmPayloadLazy.AVmPayload_notifyPayloadReady();
  };
  // Run the rpc server.
  gVmPayloadLazy.AVmPayload_runVsockRpcServer(service->asBinder().get(), service->PORT,
                               callback, /*param=*/nullptr);
}
