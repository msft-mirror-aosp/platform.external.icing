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

// Copyright 2012 Google Inc. All Rights Reserved.
// Author: ulas@google.com (Ulas Kirazci)
//
// A tool to debug the native index.

#include <getopt.h>
#include <unistd.h>

#include <string>

#include "java/com/google/android/gmscore/integ/modules/icing/jni/core/string-util.h"
#include "java/com/google/android/gmscore/integ/modules/icing/jni/index/doc-property-filter.h"
#include "java/com/google/android/gmscore/integ/modules/icing/jni/index/document-store.h"
#include "java/com/google/android/gmscore/integ/modules/icing/jni/index/dynamic-trie.h"
#include "java/com/google/android/gmscore/integ/modules/icing/jni/index/filesystem.h"
#include "java/com/google/android/gmscore/integ/modules/icing/jni/index/mobstore.h"
#include "java/com/google/android/gmscore/integ/modules/icing/jni/index/native-index-impl.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/tools/document-store-dump.h"
#include "icing/util/logging.h"

using std::vector;
using ::wireless_android_play_playlog::icing::IndexRestorationStats;

namespace icing {
namespace lib {

// 256KB for debugging.
const size_t kMaxDocumentSizeForDebugging = 1u << 18;
// Dump dynamic trie stats and contents.
void ProcessDynamicTrie(const char* filename) {
  Filesystem filesystem;
  DynamicTrie trie(filename, DynamicTrie::RuntimeOptions(), &filesystem);
  if (!trie.Init()) {
    ICING_LOG(ERROR) << IcingStringUtil::StringPrintf("Opening trie %s failed",
                                                      filename);
    return;
  }

  std::string out;
  trie.GetDebugInfo(true, &out);
  printf("Stats:\n%s", out.c_str());

  std::ostringstream contents;
  vector<std::string> keys;
  trie.DumpTrie(&contents, &keys);
  printf("Contents:\n%s", contents.str().c_str());
}

NativeIndexImpl* MakeIndex(const char* root_dir) {
  NativeConfig native_config;
  native_config.set_max_document_size(kMaxDocumentSizeForDebugging);
  FlashIndexOptions flash_index_options(
      NativeIndexImpl::GetNativeIndexDir(root_dir));
  NativeIndexImpl* ni =
      new NativeIndexImpl(root_dir, native_config, flash_index_options);
  InitStatus init_status;
  if (!ni->Init(&init_status)) {
    ICING_LOG(FATAL) << "Failed to initialize legacy native index impl";
  }

  IndexRestorationStats unused;
  ni->RestoreIndex(IndexRequestSpec::default_instance(), &unused);
  return ni;
}

void RunQuery(NativeIndexImpl* ni, const std::string& query, int start,
              int num_results) {
  // Pull out corpusids and uris.
  QueryRequestSpec spec;
  spec.set_no_corpus_filter(true);
  spec.set_want_uris(true);
  spec.set_scoring_verbosity_level(1);
  spec.set_prefix_match(true);

  QueryResponse response;
  ni->ExecuteQuery(query, spec, 10000, start, num_results, &response);

  ICING_VLOG(1) << IcingStringUtil::StringPrintf(
      "Query [%s] num results %u", query.c_str(), response.num_results());

  for (int i = 0, uri_offset = 0; i < response.num_results(); i++) {
    ICING_VLOG(1) << IcingStringUtil::StringPrintf(
        "%d: (cid=%u) uri %.*s", i, response.corpus_ids(i),
        response.uri_lengths(i), response.uri_buffer().data() + uri_offset);
    uri_offset += response.uri_lengths(i);
  }
}

void RunSuggest(NativeIndexImpl* ni, const std::string& prefix,
                int num_results) {
  SuggestionResponse results;
  ni->Suggest(prefix, num_results, vector<CorpusId>(), &results);

  ICING_VLOG(1) << IcingStringUtil::StringPrintf(
      "Query [%s] num results %zu", prefix.c_str(),
      static_cast<size_t>(results.suggestions_size()));

  for (size_t i = 0; i < results.suggestions_size(); i++) {
    ICING_VLOG(1) << IcingStringUtil::StringPrintf(
        "Sugg: [%s] display text [%s]", results.suggestions(i).query().c_str(),
        results.suggestions(i).display_text().c_str());
  }
}

int IcingTool(int argc, char** argv) {
  auto file_storage = CreatePosixFileStorage();
  enum Options {
    OPT_FILENAME,
    OPT_OP,
    OPT_QUERY,
    NUM_OPT,
  };
  static const option kOptions[NUM_OPT + 1] = {
      {"filename", 1, nullptr, 0},
      {"op", 1, nullptr, 0},
      {"query", 1, nullptr, 0},
      {nullptr, 0, nullptr, 0},
  };
  const char* opt_values[NUM_OPT];
  memset(opt_values, 0, sizeof(opt_values));

  while (true) {
    int opt_idx = -1;
    int ret = getopt_long(argc, argv, "", kOptions, &opt_idx);
    if (ret != 0) break;

    if (opt_idx >= 0 && opt_idx < NUM_OPT) {
      opt_values[opt_idx] = optarg;
    }
  }

  if (!opt_values[OPT_OP]) {
    ICING_LOG(ERROR) << "No op specified";
    return -1;
  }

  if (!opt_values[OPT_FILENAME]) {
    ICING_LOG(ERROR) << "No filename specified";
    return -1;
  }
  if (!strncmp(
          opt_values[OPT_FILENAME],
          "/data/data/com.google.android.gms/files/AppDataSearch",
          strlen("/data/data/com.google.android.gms/files/AppDataSearch"))) {
    ICING_LOG(ERROR)
        << "Should not read directly from the file in gmscore - "
           "icing-tool also commits writes as side-effects which corrupts "
           "the index on concurrent modification";
    return -1;
  }

  const char* op = opt_values[OPT_OP];
  DocumentStore::Options options(file_storage.get(),
                                 kMaxDocumentSizeForDebugging);
  if (!strcmp(op, "dyntrie")) {
    std::string full_file_path =
        absl_ports::StrCat(opt_values[OPT_FILENAME], "/idx.lexicon");
    ProcessDynamicTrie(full_file_path.c_str());
  } else if (!strcmp(op, "verify")) {
    std::unique_ptr<NativeIndexImpl> ni(MakeIndex(opt_values[OPT_FILENAME]));
    ni->CheckVerify();
  } else if (!strcmp(op, "query")) {
    if (opt_values[OPT_QUERY] == nullptr) {
      ICING_LOG(FATAL) << "Opt value is null";
    }

    std::unique_ptr<NativeIndexImpl> ni(MakeIndex(opt_values[OPT_FILENAME]));
    RunQuery(ni.get(), opt_values[OPT_QUERY], 0, 100);
  } else if (!strcmp(op, "suggest")) {
    if (opt_values[OPT_QUERY] == nullptr) {
      ICING_LOG(FATAL) << "Opt value is null";
    }

    std::unique_ptr<NativeIndexImpl> ni(MakeIndex(opt_values[OPT_FILENAME]));
    RunSuggest(ni.get(), opt_values[OPT_QUERY], 100);
  } else if (!strcmp(op, "dump-all-docs")) {
    DocumentStore ds(opt_values[OPT_FILENAME], options);
    if (!ds.Init()) {
      ICING_LOG(FATAL) << "Legacy document store failed to initialize";
    }

    printf(
        "------ Document Store Dump Start ------\n"
        "%s\n"
        "------ Document Store Dump End ------\n",
        GetDocumentStoreDump(ds).c_str());
  } else if (!strcmp(op, "dump-uris")) {
    CorpusId corpus_id = kInvalidCorpusId;
    if (opt_values[OPT_QUERY]) {
      // Query is corpus id.
      corpus_id = atoi(opt_values[OPT_QUERY]);  // NOLINT
    }
    DocumentStore ds(opt_values[OPT_FILENAME], options);
    if (!ds.Init()) {
      ICING_LOG(FATAL) << "Legacy document store failed to initialize";
    }

    DocPropertyFilter dpf;
    ds.AddDeletedTagFilter(&dpf);

    // Dump with format "<corpusid> <uri> <tagname>*".
    int filtered = 0;
    vector<std::string> tagnames;
    for (DocId document_id = 0; document_id < ds.num_documents();
         document_id++) {
      Document doc;
      if (!ds.ReadDocument(document_id, &doc)) {
        ICING_LOG(FATAL) << "Failed to read document.";
      }

      if (corpus_id != kInvalidCorpusId && corpus_id != doc.corpus_id()) {
        filtered++;
        continue;
      }
      if (dpf.Match(0, document_id)) {
        filtered++;
        continue;
      }

      tagnames.clear();
      ds.GetAllSetUserTagNames(document_id, &tagnames);

      printf("%d %s %s\n", doc.corpus_id(), doc.uri().c_str(),
             StringUtil::JoinStrings("/", tagnames).c_str());
    }
    ICING_VLOG(1) << IcingStringUtil::StringPrintf(
        "Processed %u filtered %d", ds.num_documents(), filtered);
  } else if (!strcmp(op, "dump-docs")) {
    std::string out_filename = opt_values[OPT_FILENAME];
    out_filename.append("/docs-dump");
    CorpusId corpus_id = kInvalidCorpusId;
    if (opt_values[OPT_QUERY]) {
      // Query is corpus id.
      corpus_id = atoi(opt_values[OPT_QUERY]);  // NOLINT
      out_filename.push_back('.');
      out_filename.append(opt_values[OPT_QUERY]);
    }
    DocumentStore ds(opt_values[OPT_FILENAME], options);
    if (!ds.Init()) {
      ICING_LOG(FATAL) << "Legacy document store failed to initialize";
    }

    DocPropertyFilter dpf;
    ds.AddDeletedTagFilter(&dpf);

    // Dump with format (<32-bit length><serialized content>)*.
    FILE* fp = fopen(out_filename.c_str(), "w");
    int filtered = 0;
    for (DocId document_id = 0; document_id < ds.num_documents();
         document_id++) {
      Document doc;
      if (!ds.ReadDocument(document_id, &doc)) {
        ICING_LOG(FATAL) << "Failed to read document.";
      }

      if (corpus_id != kInvalidCorpusId && corpus_id != doc.corpus_id()) {
        filtered++;
        continue;
      }
      if (dpf.Match(0, document_id)) {
        filtered++;
        continue;
      }

      std::string serialized = doc.SerializeAsString();
      uint32_t length = serialized.size();
      if (fwrite(&length, 1, sizeof(length), fp) != sizeof(length)) {
        ICING_LOG(FATAL) << "Failed to write length information to file";
      }

      if (fwrite(serialized.data(), 1, serialized.size(), fp) !=
          serialized.size()) {
        ICING_LOG(FATAL) << "Failed to write document to file";
      }
    }
    ICING_VLOG(1) << IcingStringUtil::StringPrintf(
        "Processed %u filtered %d", ds.num_documents(), filtered);
    fclose(fp);
  } else {
    ICING_LOG(ERROR) << IcingStringUtil::StringPrintf("Unknown op %s", op);
    return -1;
  }

  return 0;
}

}  // namespace lib
}  // namespace icing

int main(int argc, char** argv) { return icing::lib::IcingTool(argc, argv); }
