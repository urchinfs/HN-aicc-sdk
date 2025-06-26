/*
 *     Copyright 2022 The Urchin Authors
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

package types

import "time"

const (
	AuthHeader             = "Authorization"
	DefaultTokenExpireTime = time.Hour * 6
)

const (
	StoragePrefix          = "urchin"
	StoragePrefixAiCcToken = "urchin:storage:hnaicc:token"
	AiCcLoginName          = "pcl_test01"
)

const (
	HttpMethodGet    = "get"
	HttpMethodPost   = "post"
	HttpMethodDelete = "delete"
)

const (
	StorageListTypeFile = "file"
	StorageListTypeDir  = "directory"
)

const (
	ChunkSize      = 100 * 1024 * 1024
	ReadBufferSize = 20 * 1024
)

const (
	// AffinitySeparator is a separator of affinity.
	AffinitySeparator = "|"
)
