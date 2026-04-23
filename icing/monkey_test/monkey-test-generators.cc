// Copyright (C) 2022 Google LLC
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

#include "icing/monkey_test/monkey-test-generators.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/absl_ports/str_cat.h"
#include "icing/absl_ports/str_join.h"
#include "icing/document-builder.h"
#include "icing/join/qualified-id.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/joinable-property.h"
#include "icing/schema/section.h"

namespace icing {
namespace lib {

namespace {

constexpr std::array<PropertyConfigProto::Cardinality::Code, 3> kCardinalities =
    {PropertyConfigProto::Cardinality::REPEATED,
     PropertyConfigProto::Cardinality::OPTIONAL,
     PropertyConfigProto::Cardinality::REQUIRED};

constexpr std::array<TermMatchType::Code, 3> kTermMatchTypes = {
    TermMatchType::UNKNOWN, TermMatchType::EXACT_ONLY, TermMatchType::PREFIX};

PropertyConfigProto::Cardinality::Code GetRandomCardinality(
    MonkeyTestRandomEngine* random) {
  std::uniform_int_distribution<> dist(0, kCardinalities.size() - 1);
  return kCardinalities[dist(*random)];
}

TermMatchType::Code GetRandomIndexableTermMatchType(
    MonkeyTestRandomEngine* random) {
  std::uniform_int_distribution<> dist(1, kTermMatchTypes.size() - 1);
  return kTermMatchTypes[dist(*random)];
}

// Returns true with the given probability.
//
// REQUIRES: probability is between 0.0 and 1.0.
bool GetRandomBooleanWithProbability(MonkeyTestRandomEngine* random,
                                     float probability) {
  std::uniform_real_distribution<float> dist(0.0f, 1.0f);
  return dist(*random) < probability;
}

// Returns true with probability 50%.
bool GetRandomBoolean(MonkeyTestRandomEngine* random) {
  return GetRandomBooleanWithProbability(random, /*probability=*/0.5f);
}

// TODO: Update this function when supporting document_indexing_config.
bool IsIndexableProperty(const PropertyConfigProto& property) {
  return property.string_indexing_config().term_match_type() !=
             TermMatchType::UNKNOWN ||
         property.embedding_indexing_config().embedding_indexing_type() !=
             EmbeddingIndexingConfig::EmbeddingIndexingType::UNKNOWN;
}

bool IsJoinableProperty(const PropertyConfigProto& property) {
  return property.joinable_config().value_type() !=
         JoinableConfig::ValueType::NONE;
}

void SetStringIndexingConfig(MonkeyTestRandomEngine* random,
                             PropertyConfigProto& property, bool indexable,
                             bool joinable) {
  property.clear_string_indexing_config();
  property.clear_joinable_config();
  if (indexable) {
    StringIndexingConfig* string_indexing_config =
        property.mutable_string_indexing_config();
    string_indexing_config->set_term_match_type(
        GetRandomIndexableTermMatchType(random));
    // TODO: Try to add different TokenizerTypes. VERBATIM, RFC822, and URL are
    // the remaining candidates to consider.
    string_indexing_config->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);
  }
  if (joinable) {
    JoinableConfig* joinable_config = property.mutable_joinable_config();
    joinable_config->set_value_type(JoinableConfig::ValueType::QUALIFIED_ID);
  }
}

void SetEmbeddingIndexingConfig(MonkeyTestRandomEngine* random,
                                PropertyConfigProto& property, bool indexable) {
  property.clear_embedding_indexing_config();
  if (indexable) {
    property.mutable_embedding_indexing_config()->set_embedding_indexing_type(
        EmbeddingIndexingConfig::EmbeddingIndexingType::LINEAR_SEARCH);
  }
  if (GetRandomBoolean(random)) {
    property.mutable_embedding_indexing_config()->set_quantization_type(
        EmbeddingIndexingConfig::QuantizationType::QUANTIZE_8_BIT);
  }
}

}  // namespace

SchemaProto MonkeySchemaGenerator::GenerateSchema() {
  SchemaProto schema;
  for (int i = 0; i < config_->num_types; ++i) {
    // Generate a new type if this is the first type we're adding. Otherwise,
    // generate the type with 80% chance of generating a new type and 20%
    // chance of adding a duplicate type.
    if (num_types_generated_ == 0 ||
        GetRandomBooleanWithProbability(random_, 0.8f)) {
      *schema.add_types() = GenerateType();
    } else {
      AddDuplicateType(schema);
    }
  }
  return schema;
}

MonkeySchemaGenerator::UpdateSchemaResult MonkeySchemaGenerator::UpdateSchema(
    SchemaProto schema) {
  UpdateSchemaResult result = {.schema = std::move(schema)};
  SchemaProto& new_schema = result.schema;

  // Delete up to 2 existing types.
  std::uniform_int_distribution<> num_types_to_delete_dist(0, 2);
  for (int num_types_to_delete = num_types_to_delete_dist(*random_);
       num_types_to_delete >= 0; --num_types_to_delete) {
    if (new_schema.types_size() > 0) {
      std::uniform_int_distribution<> dist(0, new_schema.types_size() - 1);
      int index_to_delete = dist(*random_);
      result.schema_types_deleted.insert(
          new_schema.types(index_to_delete).schema_type());
      new_schema.mutable_types()->SwapElements(index_to_delete,
                                               new_schema.types_size() - 1);
      new_schema.mutable_types()->RemoveLast();
    }
  }

  // Updating about 1/3 of existing types.
  for (int i = 0; i < new_schema.types_size(); ++i) {
    std::uniform_int_distribution<> dist(0, 2);
    if (dist(*random_) == 0) {
      UpdateType(*new_schema.mutable_types(i), result);
    }
  }

  // Add up to 5 new types.
  std::uniform_int_distribution<> num_types_to_add_dist(0, 5);
  for (int num_types_to_add = num_types_to_add_dist(*random_);
       num_types_to_add >= 0; --num_types_to_add) {
    // 20% chance of adding a duplicate type.
    if (GetRandomBooleanWithProbability(random_, 0.2f)) {
      AddDuplicateType(new_schema);
    } else {
      *new_schema.add_types() = GenerateType();
    }
  }

  return result;
}

void MonkeySchemaGenerator::ReloadPreviousStatus(const SchemaProto& schema) {
  int max_schema_id = 0;
  for (const SchemaTypeConfigProto& type_config : schema.types()) {
    max_schema_id =
        std::max(max_schema_id, std::stoi(type_config.schema_type().substr(
                                    kSchemaTypeNamePrefix.size())));

    // To reset num_properties_generated_ according to the previous run, we use
    // the maximum property_id + 1 as an estimate.
    int max_property_id = 0;
    for (const PropertyConfigProto& property : type_config.properties()) {
      max_property_id =
          std::max(max_property_id, std::stoi(property.property_name().substr(
                                        kSchemaPropertyNamePrefix.size())));
    }
    num_properties_generated_[type_config.schema_type()] = max_property_id + 1;
  }
  // To reset num_types_generated_ according to the previous run, we use the
  // maximum schema_id + 1 as an estimate.
  num_types_generated_ = max_schema_id + 1;
}

PropertyConfigProto MonkeySchemaGenerator::GenerateProperty(
    const SchemaTypeConfigProto& type_config,
    PropertyConfigProto::Cardinality::Code cardinality, bool indexable,
    bool joinable) {
  PropertyConfigProto prop;
  prop.set_property_name(
      std::string(kSchemaPropertyNamePrefix) +
      std::to_string(num_properties_generated_[type_config.schema_type()]++));
  // TODO: Perhaps in future iterations we will want to generate more types of
  // properties.
  // - Currently, we are generating either a string or a vector property.
  // - Currently we only have qualified id joinable properties, so if it is
  //   joinable, then it has to be a string property.
  if (joinable || GetRandomBoolean(random_)) {
    prop.set_data_type(PropertyConfigProto::DataType::STRING);
    SetStringIndexingConfig(random_, prop, indexable, joinable);
  } else {
    prop.set_data_type(PropertyConfigProto::DataType::VECTOR);
    SetEmbeddingIndexingConfig(random_, prop, indexable);
  }
  prop.set_cardinality(cardinality);
  return prop;
}

void MonkeySchemaGenerator::UpdateProperty(
    const SchemaTypeConfigProto& type_config, PropertyConfigProto& property,
    UpdateSchemaResult& result) {
  PropertyConfigProto::Cardinality::Code new_cardinality =
      GetRandomCardinality(random_);
  if (new_cardinality != property.cardinality()) {
    // Only do compatible cardinality update for now, otherwise it would be hard
    // to track which documents will be invalid after updating the schema.
    //
    // The following type of updates are not allowed:
    // - optional -> required
    // - repeated -> optional
    // - repeated -> required
    if (property.cardinality() == PropertyConfigProto::Cardinality::OPTIONAL &&
        new_cardinality == PropertyConfigProto::Cardinality::REQUIRED) {
      return;
    }
    if (property.cardinality() == PropertyConfigProto::Cardinality::REPEATED &&
        (new_cardinality == PropertyConfigProto::Cardinality::OPTIONAL ||
         new_cardinality == PropertyConfigProto::Cardinality::REQUIRED)) {
      return;
    }
    property.set_cardinality(new_cardinality);
  }

  bool old_indexable = IsIndexableProperty(property);
  bool new_indexable = GetRandomBoolean(random_);
  bool old_joinable = IsJoinableProperty(property);
  // 20% chance to flip joinable. Only works for string properties.
  bool new_joinable = old_joinable;
  if (config_->IsJoinEnabled() &&
      GetRandomBooleanWithProbability(random_, 0.2f)) {
    new_joinable = !old_joinable;
  }

  bool index_incompatible = old_indexable != new_indexable;
  bool join_incompatible = old_joinable != new_joinable;

  if (property.data_type() == PropertyConfigProto::DataType::STRING) {
    TermMatchType::Code old_term_match_type =
        property.string_indexing_config().term_match_type();
    JoinableConfig::ValueType::Code old_joinable_value_type =
        property.joinable_config().value_type();

    SetStringIndexingConfig(random_, property, new_indexable, new_joinable);

    TermMatchType::Code new_term_match_type =
        property.string_indexing_config().term_match_type();
    JoinableConfig::ValueType::Code new_joinable_value_type =
        property.joinable_config().value_type();

    if (old_term_match_type != new_term_match_type) {
      index_incompatible = true;
    }
    if (old_joinable_value_type != new_joinable_value_type) {
      join_incompatible = true;
    }
  } else if (property.data_type() == PropertyConfigProto::DataType::VECTOR) {
    EmbeddingIndexingConfig::QuantizationType::Code old_quantization_type =
        property.embedding_indexing_config().quantization_type();
    SetEmbeddingIndexingConfig(random_, property, new_indexable);
    EmbeddingIndexingConfig::QuantizationType::Code new_quantization_type =
        property.embedding_indexing_config().quantization_type();
    if (old_quantization_type != new_quantization_type) {
      index_incompatible = true;
    }
  }
  if (index_incompatible) {
    result.schema_types_index_incompatible.insert(type_config.schema_type());
  }
  if (join_incompatible) {
    result.schema_types_join_incompatible.insert(type_config.schema_type());
  }
}

SchemaTypeConfigProto MonkeySchemaGenerator::GenerateType() {
  SchemaTypeConfigProto type_config;
  type_config.set_schema_type(std::string(kSchemaTypeNamePrefix) +
                              std::to_string(num_types_generated_++));
  std::uniform_int_distribution<> possible_num_properties_dist(
      0, static_cast<int>(config_->possible_num_properties.size()) - 1);
  int total_num_properties =
      config_->possible_num_properties[possible_num_properties_dist(*random_)];

  int num_indexed_properties = 0;
  int num_join_properties = 0;
  for (int i = 0; i < total_num_properties; ++i) {
    // Decide whether this property is indexable.
    bool indexable = false;
    if (num_indexed_properties < kTotalNumSections) {
      indexable = GetRandomBoolean(random_);
    }
    if (indexable) {
      num_indexed_properties += 1;
    }

    // Decide whether this property is joinable.
    bool joinable = false;
    if (config_->IsJoinEnabled() &&
        num_join_properties < kTotalNumJoinableProperties) {
      // 40% chance of getting a joinable property.
      joinable = GetRandomBooleanWithProbability(random_, 0.4f);
    }
    if (joinable) {
      num_join_properties += 1;
    }

    (*type_config.add_properties()) = GenerateProperty(
        type_config, GetRandomCardinality(random_), indexable, joinable);
  }
  return type_config;
}

void MonkeySchemaGenerator::AddDuplicateType(SchemaProto& schema) {
  if (schema.types_size() == 0) {
    return;
  }

  // Make the added type a dupe of one of the first 3 types.
  std::uniform_int_distribution<> duplicate_type_index_dist(
      0, std::min(2, schema.types_size() - 1));
  int duplicate_type_index = duplicate_type_index_dist(*random_);

  const SchemaTypeConfigProto& base_type =
      schema.types().at(duplicate_type_index);
  SchemaTypeConfigProto new_dupe_type = base_type;
  new_dupe_type.set_schema_type(std::string(kSchemaTypeNamePrefix) +
                                std::to_string(num_types_generated_++));
  int max_property_id = 0;
  for (const PropertyConfigProto& property : new_dupe_type.properties()) {
    max_property_id =
        std::max(max_property_id, std::stoi(property.property_name().substr(
                                      kSchemaPropertyNamePrefix.size())));
  }
  num_properties_generated_[new_dupe_type.schema_type()] = max_property_id + 1;
  *schema.add_types() = std::move(new_dupe_type);
}

void MonkeySchemaGenerator::UpdateType(SchemaTypeConfigProto& type_config,
                                       UpdateSchemaResult& result) {
  // Delete up to 4 existing property.
  std::uniform_int_distribution<> num_properties_to_delete_dist(0, 4);
  for (int num_properties_to_delete = num_properties_to_delete_dist(*random_);
       num_properties_to_delete >= 0; --num_properties_to_delete) {
    if (type_config.properties_size() > 0) {
      std::uniform_int_distribution<> dist(0,
                                           type_config.properties_size() - 1);
      int index_to_delete = dist(*random_);
      // Only delete a required property for now, otherwise it would be hard
      // to track which documents will be invalid after updating the schema.
      if (type_config.properties(index_to_delete).cardinality() !=
          PropertyConfigProto::Cardinality::REQUIRED) {
        continue;
      }
      if (IsIndexableProperty(type_config.properties(index_to_delete))) {
        result.schema_types_index_incompatible.insert(
            type_config.schema_type());
      }
      if (IsJoinableProperty(type_config.properties(index_to_delete))) {
        result.schema_types_join_incompatible.insert(type_config.schema_type());
      }
      // Removing a property will cause the type to be considered as
      // incompatible.
      result.schema_types_incompatible.insert(type_config.schema_type());

      type_config.mutable_properties()->SwapElements(
          index_to_delete, type_config.properties_size() - 1);
      type_config.mutable_properties()->RemoveLast();
    }
  }

  // Updating about 1/3 of existing properties.
  for (int i = 0; i < type_config.properties_size(); ++i) {
    if (GetRandomBooleanWithProbability(random_, 0.3333333333f)) {
      UpdateProperty(type_config, *type_config.mutable_properties(i), result);
    }
  }

  // Add up to 4 new properties.
  std::uniform_int_distribution<> num_types_to_add_dist(0, 4);
  for (int num_types_to_add = num_types_to_add_dist(*random_);
       num_types_to_add >= 0; --num_types_to_add) {
    PropertyConfigProto::Cardinality::Code new_cardinality =
        GetRandomCardinality(random_);
    // Adding a required property will make all document of this type invalid.
    if (new_cardinality == PropertyConfigProto::Cardinality::REQUIRED) {
      result.schema_types_incompatible.insert(type_config.schema_type());
    }
    bool indexable = GetRandomBoolean(random_);
    bool joinable = config_->IsJoinEnabled()
                        ? GetRandomBooleanWithProbability(random_, 0.4f)
                        : false;
    PropertyConfigProto new_property =
        GenerateProperty(type_config, new_cardinality, indexable, joinable);
    if (indexable) {
      result.schema_types_index_incompatible.insert(type_config.schema_type());
    }
    if (joinable) {
      result.schema_types_join_incompatible.insert(type_config.schema_type());
    }
    (*type_config.add_properties()) = std::move(new_property);
  }

  int num_indexed_properties = 0;
  int num_join_properties = 0;
  for (int i = 0; i < type_config.properties_size(); ++i) {
    if (IsIndexableProperty(type_config.properties(i))) {
      ++num_indexed_properties;
    }
    if (IsJoinableProperty(type_config.properties(i))) {
      ++num_join_properties;
    }
  }

  if (num_indexed_properties > kTotalNumSections ||
      num_join_properties > kTotalNumJoinableProperties) {
    result.is_invalid_schema = true;
  }
}

std::string MonkeyDocumentGenerator::GetNamespace() const {
  // When num_namespaces is 0, all documents generated get different namespaces.
  // Otherwise, namespaces will be randomly picked from a set with
  // num_namespaces elements.
  if (config_->num_namespaces == 0) {
    return absl_ports::StrCat("namespace", std::to_string(num_docs_generated_));
  }
  return GetNamespaceWithRange(0, config_->num_namespaces);
}

std::string MonkeyDocumentGenerator::GetNamespaceWithRange(int l, int r) const {
  std::uniform_int_distribution<> dist(l, r - 1);
  return absl_ports::StrCat("namespace", std::to_string(dist(*random_)));
}

std::string MonkeyDocumentGenerator::GetUri() const {
  // When num_uris is 0, all documents generated get different URIs. Otherwise,
  // URIs will be randomly picked from a set with num_uris elements.
  if (config_->num_uris == 0) {
    return absl_ports::StrCat(kDocumentUriPrefix,
                              std::to_string(num_docs_generated_));
  }
  return GetUriWithRange(0, config_->num_uris);
}

std::string MonkeyDocumentGenerator::GetUriWithRange(int l, int r) const {
  std::uniform_int_distribution<> dist(l, r - 1);
  return absl_ports::StrCat(kDocumentUriPrefix, std::to_string(dist(*random_)));
}

int MonkeyDocumentGenerator::GetNumTokens() const {
  std::uniform_int_distribution<> dist(0,
                                       config_->possible_num_tokens.size() - 1);
  int n = config_->possible_num_tokens[dist(*random_)];
  // Add some noise
  std::uniform_real_distribution<> real_dist(0.5, 1);
  float p = real_dist(*random_);
  return n * p;
}

std::vector<std::string> MonkeyDocumentGenerator::GetStringPropertyContent()
    const {
  int num_tokens = GetNumTokens();
  std::vector<std::string> content;
  content.reserve(num_tokens);
  while (num_tokens) {
    content.push_back(std::string(GetToken()));
    --num_tokens;
  }
  return content;
}

std::vector<std::string> MonkeyDocumentGenerator::GetQualifiedIds(
    PropertyConfigProto::Cardinality::Code cardinality) const {
  assert(config_->IsJoinEnabled());

  int num_tokens = 1;
  if (cardinality == PropertyConfigProto::Cardinality::REPEATED) {
    num_tokens = GetNumTokens();
  }

  std::uniform_int_distribution<> dist(
      0, static_cast<int>(
             config_->possible_ref_qualified_id_random_spaces.size()) -
             1);

  std::vector<std::string> content;
  content.reserve(num_tokens);
  for (int i = 0; i < num_tokens; ++i) {
    const IcingMonkeyTestRunnerConfiguration::QualifiedIdRandomSpace&
        random_space =
            config_->possible_ref_qualified_id_random_spaces[dist(*random_)];

    std::string name_space = GetNamespaceWithRange(random_space.namespace_l,
                                                   random_space.namespace_r);
    std::string uri = GetUriWithRange(random_space.uri_l, random_space.uri_r);
    content.push_back(
        QualifiedId(std::move(name_space), std::move(uri)).ToString());
  }
  return content;
}

int MonkeyDocumentGenerator::GetNumVectors(
    PropertyConfigProto::Cardinality::Code cardinality) const {
  if (cardinality == PropertyConfigProto::Cardinality::REQUIRED) {
    return 1;
  } else if (cardinality == PropertyConfigProto::Cardinality::OPTIONAL) {
    std::uniform_int_distribution<> dist(0, 1);
    return dist(*random_);
  }

  // For repeated properties:
  std::uniform_int_distribution<> dist(
      0, config_->possible_num_vectors.size() - 1);
  int n = config_->possible_num_vectors[dist(*random_)];
  // Add some noise
  std::uniform_real_distribution<> real_dist(0.5, 1);
  float p = real_dist(*random_);
  return n * p;
}

PropertyProto::VectorProto MonkeyDocumentGenerator::GetRandomVector() const {
  std::uniform_int_distribution<> dimension_dist(
      0, config_->possible_vector_dimensions.size() - 1);
  std::uniform_real_distribution<float> value_dist(-1.0, 1.0);

  PropertyProto::VectorProto vector;
  vector.set_model_signature("model");
  int dimension = config_->possible_vector_dimensions[dimension_dist(*random_)];
  for (int i = 0; i < dimension; ++i) {
    vector.add_values(value_dist(*random_));
  }
  return vector;
}

std::vector<PropertyProto::VectorProto>
MonkeyDocumentGenerator::GetVectorPropertyContent(
    PropertyConfigProto::Cardinality::Code cardinality) const {
  int num_vectors = GetNumVectors(cardinality);
  std::vector<PropertyProto::VectorProto> content;
  content.reserve(num_vectors);
  while (num_vectors) {
    content.push_back(GetRandomVector());
    --num_vectors;
  }
  return content;
}

MonkeyTokenizedDocument MonkeyDocumentGenerator::GenerateDocument() {
  MonkeyTokenizedDocument document;
  const SchemaTypeConfigProto& type_config = GetType();
  std::string name_space = GetNamespace();
  DocumentBuilder doc_builder =
      DocumentBuilder()
          .SetNamespace(name_space)
          .SetSchema(type_config.schema_type())
          .SetUri(GetUri())
          .SetCreationTimestampMs(clock_.GetSystemTimeMilliseconds());
  for (const PropertyConfigProto& prop : type_config.properties()) {
    if (prop.data_type() == PropertyConfigProto::DataType::STRING) {
      // Generate string contents.
      std::vector<std::string> prop_content;

      bool generate_qualified_ids = false;
      if (config_->IsJoinEnabled()) {
        // - If the property is a qualified id joinable property, generate
        //   qualified id(s) for this property content.
        // - If it is not a qualified id joinable property, 30% chance to
        //   generate qualified id(s) for this property content.
        //
        // Note: a string property can have qualified id(s) as content even if
        //   it is not a joinable property currently. Later UpdateSchema may
        //   flip a non-joinable property to joinable, so the existing qualified
        //   ids should become joinable.
        generate_qualified_ids = (prop.joinable_config().value_type() ==
                                  JoinableConfig::ValueType::QUALIFIED_ID) ||
                                 GetRandomBooleanWithProbability(random_, 0.3f);
      }
      if (generate_qualified_ids) {
        prop_content = GetQualifiedIds(prop.cardinality());
      } else {
        prop_content = GetStringPropertyContent();
        if (prop.cardinality() != PropertyConfigProto::Cardinality::REPEATED) {
          // If the property cardinality only allows 1 value, then concatenate
          // them into one single string (separated by spaces). Otherwise, leave
          // them as multiple string values.
          std::string prop_content_str = absl_ports::StrJoin(prop_content, " ");
          prop_content = std::vector<std::string>{prop_content_str};
        }
      }

      // Add to the document proto.
      doc_builder.AddStringProperty(prop.property_name(), prop_content.cbegin(),
                                    prop_content.cend());

      // No matter whether the property is indexable currently, we have to
      // create a section for it since a non-indexable property can become
      // indexable after a schema type change. The in-memory icing will
      // automatically skip sections that are non-indexable at the time of
      // search requests.
      //
      // Note: qualified id joinable properties could also be indexable, so we
      //   don't distinguish between them and normal string properties here.
      MonkeySection section = {.path = prop.property_name(),
                               .string_values = std::move(prop_content)};
      document.sections.push_back(std::move(section));
    } else {
      std::vector<PropertyProto::VectorProto> prop_content =
          GetVectorPropertyContent(prop.cardinality());
      doc_builder.AddVectorProperty(prop.property_name(), prop_content);

      // Similar to the string property, no matter whether the property is
      // indexable currently, we have to create a section for it.
      MonkeySection section = {.path = prop.property_name(),
                               .vector_values = std::move(prop_content)};
      document.sections.push_back(std::move(section));
    }
  }
  document.document = doc_builder.Build();
  ++num_docs_generated_;
  return document;
}

}  // namespace lib
}  // namespace icing
