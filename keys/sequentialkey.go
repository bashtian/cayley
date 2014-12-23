// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keys

import (
	"strconv"
	"sync"

	"github.com/bashtian/cayley/graph"
)

type Sequential struct {
	nextID int64
	mut    sync.Mutex
}

func NewSequentialKey(horizon int64) graph.PrimaryKey {
	if horizon <= 0 {
		horizon = 1
	}
	return &Sequential{nextID: horizon}
}

func (s *Sequential) Next() graph.PrimaryKey {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.nextID++
	if s.nextID <= 0 {
		s.nextID = 1
	}
	return s
}

func (s *Sequential) Int() int64 {
	return s.nextID
}

func (s *Sequential) String() string {
	return strconv.FormatInt(s.nextID, 10)
}
