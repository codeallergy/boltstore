/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package boltstore_test

import (
	"context"
	"github.com/codeallergy/store"
	"github.com/stretchr/testify/require"
	"github.com/codeallergy/boltstore"
	"log"
	"os"
	"testing"
)

func TestPrimitives(t *testing.T) {

	file, err := os.CreateTemp(os.TempDir(), "boltdatabasetest.*.db")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())

	s, err := boltstore.New("test", file.Name(), os.FileMode(0666))
	require.NoError(t, err)

	defer s.Destroy()

	bucket := "first"

	err = s.Set(context.Background()).ByKey("%s:name", bucket).String("value")
	require.NoError(t, err)

	value, err := s.Get(context.Background()).ByKey("%s:name", bucket).ToString()
	require.NoError(t, err)

	require.Equal(t,"value", value)

	cnt := 0
	err = s.Enumerate(context.Background()).Do(func(entry *store.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate(context.Background()).ByPrefix("%s:", bucket).Do(func(entry *store.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate(context.Background()).ByPrefix("%s:n", bucket).Do(func(entry *store.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate(context.Background()).ByPrefix("%s:name", bucket).Do(func(entry *store.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate(context.Background()).ByPrefix("%s:n", bucket).Seek("%s:name", bucket).Do(func(entry *store.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate(context.Background()).ByPrefix("%s:nothing", bucket).Do(func(entry *store.RawEntry) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 0, cnt)

}