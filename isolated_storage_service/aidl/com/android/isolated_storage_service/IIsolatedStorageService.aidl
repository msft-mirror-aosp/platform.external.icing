/*
 * Copyright 2025 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.android.isolated_storage_service;

import com.android.isolated_storage_service.IIcingSearchEngine;

/**
 * Isolated Storage Service is a service that runs storage services in an isolated pVM environment.
 */
interface IIsolatedStorageService {
  const long PORT = 1688;

  /**
   * Quits the VM.
   */
  oneway void quit();

  /**
   * Trims the memory used by the pVM.
   */
  oneway void trimMemory();

  /**
   * Returns an Icing connection for the given userId. Creates a new Icing connection if one does
   * not already exist for the given userId.
   *
   * @param userId The userId of the caller.
   * @return An Icing connection for the given userId.
   */
  IIcingSearchEngine getOrCreateIcingConnection(int userId);

  /**
   * Removes an Icing connection for the given userId.
   *
   * @param userId The userId of the caller.
   */
  oneway void removeIcingConnection(int userId);
}
