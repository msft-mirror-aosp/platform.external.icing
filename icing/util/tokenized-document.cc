// Copyright (C) 2020 Google LLC
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

#include "third_party/icing/util/tokenized-document.h"

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/proto/document.proto.h"
#include "third_party/icing/proto/document_wrapper.proto.h"
#include "third_party/icing/schema/joinable-property.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/schema/section.h"
#include "third_party/icing/tokenization/language-segmenter.h"
#include "third_party/icing/tokenization/token.h"
#include "third_party/icing/tokenization/tokenizer-factory.h"
#include "third_party/icing/tokenization/tokenizer.h"
#include "third_party/icing/util/document-util.h"
#include "third_party/icing/util/document-validator.h"
#include "third_party/icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

libtextclassifier3::StatusOr<std::vector<TokenizedSection>> Tokenize(
    const SchemaStore* schema_store,
    const LanguageSegmenter* language_segmenter,
    const std::vector<Section<std::string_view>>& string_sections) {
  std::vector<TokenizedSection> tokenized_string_sections;
  std::vector<Token> batch_tokens;
  for (const Section<std::string_view>& section : string_sections) {
    ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer> tokenizer,
                           tokenizer_factory::CreateIndexingTokenizer(
                               section.metadata.tokenizer, language_segmenter));
    std::vector<std::string_view> token_sequence;
    for (std::string_view subcontent : section.content) {
      ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer->Tokenize(subcontent));
      while (itr->Advance()) {
        itr->GetTokens(&batch_tokens);
        for (const Token& token : batch_tokens) {
          token_sequence.push_back(token.text);
        }
      }
    }
    tokenized_string_sections.emplace_back(SectionMetadata(section.metadata),
                                           std::move(token_sequence));
  }

  return tokenized_string_sections;
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<TokenizedDocument>
TokenizedDocument::Create(const SchemaStore* schema_store,
                          const LanguageSegmenter* language_segmenter,
                          int64_t current_time_ms, DocumentProto document) {
  // Set the creation timestamp if it is not set.
  if (document.creation_timestamp_ms() == 0) {
    document.set_creation_timestamp_ms(current_time_ms);
  }

  // Since there are many std::string_view objects pointing to the document
  // proto, we should make sure DocumentProto in DocumentWrapper has a fixed
  // address. The simplest way is to use a unique_ptr.
  auto document_wrapper_ptr = std::make_unique<DocumentWrapper>(
      document_util::CreateDocumentWrapper(std::move(document)));

  DocumentValidator validator(schema_store);
  ICING_RETURN_IF_ERROR(validator.Validate(document_wrapper_ptr->document()));

  ICING_ASSIGN_OR_RETURN(
      SectionGroup section_group,
      schema_store->ExtractSections(document_wrapper_ptr->document()));

  ICING_ASSIGN_OR_RETURN(JoinablePropertyGroup joinable_property_group,
                         schema_store->ExtractJoinableProperties(
                             document_wrapper_ptr->document()));

  // Tokenize string sections
  ICING_ASSIGN_OR_RETURN(
      std::vector<TokenizedSection> tokenized_string_sections,
      Tokenize(schema_store, language_segmenter,
               section_group.string_sections));

  TokenizedDocument tokenized_document(
      std::move(document_wrapper_ptr), std::move(tokenized_string_sections),
      std::move(section_group.integer_sections),
      std::move(section_group.vector_sections),
      std::move(joinable_property_group));

  // Set the num_string_tokens into the document proto.
  int32_t num_string_tokens = tokenized_document.num_string_tokens();
  tokenized_document.document_wrapper_->mutable_document()
      ->mutable_internal_fields()
      ->set_length_in_tokens(num_string_tokens);

  return tokenized_document;
}

}  // namespace lib
}  // namespace icing
