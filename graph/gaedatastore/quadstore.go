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

// +build appengine

package gaedatastore

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"hash"
	"math"
	"net/http"
	"time"

	"github.com/barakmich/glog"
	"github.com/mjibson/goon"

	"appengine"
	"appengine/datastore"
	"appengine/memcache"

	"github.com/bashtian/cayley/graph"
	"github.com/bashtian/cayley/graph/iterator"
	"github.com/bashtian/cayley/keys"
	"github.com/bashtian/cayley/quad"
)

const (
	QuadStoreType = "gaedatastore"
	quadKind      = "quad"
	nodeKind      = "node"
)

var (
	cache     = map[string]Cache{}
	cacheTime = time.Now()
	// Order of quad fields
	spo = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
)

type QuadStore struct {
	hashSize   int
	makeHasher func() hash.Hash
	context    appengine.Context
	db         *goon.Goon
	//cache      map[string]Cache
}

type Cache struct {
	buffer []string
	last   string
	done   bool
}

type MetadataEntry struct {
	Id        string `datastore:"-" goon:"id"`
	_kind     string `goon:"kind,metadata"`
	NodeCount int64
	QuadCount int64
}

type Token struct {
	Kind string
	Hash string
}

type QuadEntry struct {
	Id        string `datastore:"-" goon:"id"`
	Kind      string `datastore:"-" goon:"kind,quad"`
	Hash      string
	Added     []int64 `datastore:",noindex"`
	Deleted   []int64 `datastore:",noindex"`
	Subject   string  `datastore:"subject"`
	Predicate string  `datastore:"predicate"`
	Object    string  `datastore:"object"`
	Label     string  `datastore:"label"`
}

type NodeEntry struct {
	Id    string `datastore:"-" goon:"id"`
	_kind string `goon:"kind,node"`
	Name  string
	Size  int64
}

type LogEntry struct {
	Id        int64  `datastore:"-" goon:"id"`
	_kind     string `goon:"kind,logentry"`
	LogID     int64
	Action    string
	Key       string
	Timestamp int64
}

func init() {
	graph.RegisterQuadStore("gaedatastore", true, newQuadStore, initQuadStore, newQuadStoreForRequest)
}

func (qs *QuadStore) resetCache() {
	cache = make(map[string]Cache)
	cacheTime = time.Now()
	t, _ := cacheTime.MarshalBinary()
	item := &memcache.Item{
		Key:   "last_cache_reset",
		Value: t,
	}
	// Add the item to the memcache, if the key does not already exist
	if err := memcache.Add(qs.db.Context, item); err == memcache.ErrNotStored {
		qs.db.Context.Infof("item with key %q already exists", item.Key)
	} else if err != nil {
		qs.db.Context.Errorf("error adding item: %v", err)
	}
}

func (qs *QuadStore) checkCache() {
	// Get the item from the memcache
	if item, err := memcache.Get(qs.db.Context, "last_cache_reset"); err != nil {
		qs.resetCache()
	} else {
		t := time.Time{}
		t.UnmarshalBinary(item.Value)
		qs.db.Context.Infof("the lyric is %q", item.Value)
		if cacheTime.Before(time.Now()) {
			qs.resetCache()
		}
	}
}

func initQuadStore(_ string, _ graph.Options) error {
	// TODO (stefankoshiw) check appengine datastore for consistency
	return nil
}

func newQuadStore(_ string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	qs.hashSize = sha1.Size
	qs.makeHasher = sha1.New
	return &qs, nil
}

func newQuadStoreForRequest(qs graph.QuadStore, options graph.Options) (graph.QuadStore, error) {
	newQs, err := newQuadStore("", options)
	if err != nil {
		return nil, err
	}
	t := newQs.(*QuadStore)
	t.context, err = getContext(options)
	t.db = goon.FromContext(t.context)
	return newQs, err
}

func (qs *QuadStore) createKeyForQuad(q quad.Quad) *datastore.Key {
	hasher := qs.makeHasher()
	id := qs.hashOf(q.Subject, hasher)
	id += qs.hashOf(q.Predicate, hasher)
	id += qs.hashOf(q.Object, hasher)
	id += qs.hashOf(q.Label, hasher)
	return qs.createKeyFromToken(&Token{quadKind, id})
}

func (qs *QuadStore) createKeyForNode(n string) *datastore.Key {
	hasher := qs.makeHasher()
	id := qs.hashOf(n, hasher)
	return qs.createKeyFromToken(&Token{nodeKind, id})
}

func (qs *QuadStore) createKeyForMetadata() *datastore.Key {
	return qs.createKeyFromToken(&Token{"metadata", "metadataentry"})
}

func (qs *QuadStore) createKeyForLog(deltaID int64) *datastore.Key {
	return datastore.NewKey(qs.context, "logentry", "", deltaID, nil)
}

func (qs *QuadStore) createKeyFromToken(t *Token) *datastore.Key {
	return datastore.NewKey(qs.context, t.Kind, t.Hash, 0, nil)
}

func (qs *QuadStore) checkValid(k *datastore.Key) (bool, error) {
	q := QuadEntry{Id: k.StringID(), Kind: k.Kind()}
	//err := datastore.Get(qs.context, k, &q)
	err := qs.db.Get(&q)
	//qs.db.Context.Infof("checkValid %v %#v err:%v", k.StringID(), q, err)

	if err == datastore.ErrNoSuchEntity || len(q.Added) <= len(q.Deleted) {
		return false, nil
	}

	if _, ok := err.(*datastore.ErrFieldMismatch); ok {
		return true, nil
	}
	if err != nil {
		glog.Warningf("Error occured when getting quad/node %s %v", k, err)
		return false, err
	}
	return true, nil
}

func getContext(opts graph.Options) (appengine.Context, error) {
	req := opts["HTTPRequest"].(*http.Request)
	if req == nil {
		err := errors.New("HTTP Request needed")
		glog.Fatalln(err)
		return nil, err
	}
	return appengine.NewContext(req), nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta) error {
	if qs.context == nil {
		return errors.New("No context, graph not correctly initialised")
	}
	toKeep := make([]graph.Delta, 0)
	for _, d := range in {
		key := qs.createKeyForQuad(d.Quad)
		keep := false
		//qs.db.Context.Infof("ApplyDeltas %v %+v", key.Kind(), d)

		switch d.Action {
		case graph.Add:
			if found, err := qs.checkValid(key); !found && err == nil {
				keep = true
			} else if err != nil {
				return err
			} else {
				glog.Warningf("Quad exists already: %v", d)
			}
		case graph.Delete:
			if found, err := qs.checkValid(key); found && err == nil {
				keep = true
			} else if err != nil {
				return err
			} else {
				glog.Warningf("Quad does not exist and so cannot be deleted: %v", d)
			}
		default:
			keep = true
		}
		//qs.context.Infof("ApplyDeltas keep %v %v", keep, d.Quad)

		if keep {
			toKeep = append(toKeep, d)
		}
	}
	if len(toKeep) == 0 {
		return nil
	}
	qs.resetCache()
	err := qs.updateLog(toKeep)
	if err != nil {
		glog.Errorf("Updating log failed %v", err)
		return err
	}

	if glog.V(2) {
		glog.Infoln("Existence verified. Proceeding.")
	}

	quadsAdded, err := qs.updateQuads(toKeep)
	//qs.db.Context.Infof("quads added %v", quadsAdded)
	if err != nil {
		glog.Errorf("UpdateQuads failed %v", err)
		return err
	}
	nodesAdded, err := qs.updateNodes(toKeep)
	//qs.db.Context.Infof("nodesAdded %v", nodesAdded)

	if err != nil {
		glog.Warningf("UpdateNodes failed %v", err)
		return err
	}
	err = qs.updateMetadata(quadsAdded, nodesAdded)
	if err != nil {
		glog.Warningf("UpdateMetadata failed %v", err)
		return err
	}
	return nil
}

func (qs *QuadStore) updateNodes(in []graph.Delta) (int64, error) {
	// Collate changes to each node
	var countDelta int64
	var nodesAdded int64
	nodeDeltas := make(map[string]int64)
	for _, d := range in {
		if d.Action == graph.Add {
			countDelta = 1
		} else {
			countDelta = -1
		}
		nodeDeltas[d.Quad.Subject] += countDelta
		nodeDeltas[d.Quad.Object] += countDelta
		nodeDeltas[d.Quad.Predicate] += countDelta
		if d.Quad.Label != "" {
			nodeDeltas[d.Quad.Label] += countDelta
		}
		nodesAdded += countDelta
	}
	// Create keys and new nodes
	tempNodes := make([]NodeEntry, 0, len(nodeDeltas))
	for k, v := range nodeDeltas {
		key := qs.createKeyForNode(k)
		n := NodeEntry{Id: key.StringID(), _kind: key.Kind(), Name: k, Size: v}
		tempNodes = append(tempNodes, n)
	}
	// In accordance with the appengine datastore spec, cross group transactions
	// like these can only be done in batches of 5
	for i := 0; i < len(nodeDeltas); i += 5 {
		j := int(math.Min(float64(len(nodeDeltas)-i), 5))
		foundNodes := make([]NodeEntry, j)
		for k := range foundNodes {
			foundNodes[k].Id = tempNodes[i+k].Id
		}
		err := qs.db.RunInTransaction(func(tg *goon.Goon) error {
			err := tg.GetMulti(foundNodes)
			// Sift through for errors
			if me, ok := err.(appengine.MultiError); ok {
				for _, merr := range me {
					if merr != nil && merr != datastore.ErrNoSuchEntity {
						glog.Errorf("Error: %v", merr)
						return merr
					}
				}
			}
			// Carry forward the sizes of the nodes from the datastore
			for k, _ := range foundNodes {
				if foundNodes[k].Name != "" {
					//qs.db.Context.Infof("foundNode %v %v", tempNodes[i+k], foundNodes[k])
					tempNodes[i+k].Size += foundNodes[k].Size
				}
			}
			_, err = tg.PutMulti(tempNodes[i : i+j])
			return err
		}, &datastore.TransactionOptions{XG: true})
		if err != nil {
			glog.Errorf("Error: %v", err)
			return 0, err
		}
	}

	return nodesAdded, nil
}

func (qs *QuadStore) updateQuads(in []graph.Delta) (int64, error) {
	foundQuads := make([]QuadEntry, len(in))
	for i, d := range in {
		key := qs.createKeyForQuad(d.Quad)
		foundQuads[i].Id = key.StringID()
		foundQuads[i].Kind = key.Kind()
	}

	/*	keys := make([]*datastore.Key, 0, len(in))
		for _, d := range in {
			keys = append(keys, qs.createKeyForQuad(d.Quad))
		}*/
	var quadCount int64
	for i := 0; i < len(in); i += 5 {
		// Find the closest batch of 5
		j := int(math.Min(float64(len(in)-i), 5))
		err := qs.db.RunInTransaction(func(tg *goon.Goon) error {
			//foundQuads := make([]QuadEntry, j)
			// We don't process errors from GetMulti as they don't mean anything,
			// we've handled existing quad conflicts above and we overwrite everything again anyways

			tg.GetMulti(foundQuads[i : i+j])
			//datastore.GetMulti(c, keys, foundQuads)
			for k, _ := range foundQuads[i : i+j] {
				x := i + k
				//qs.db.Context.Infof("updateQuads %v %+v %+v", k, foundQuads[x], in[x])
				foundQuads[x].Hash = foundQuads[x].Id
				foundQuads[x].Subject = in[x].Quad.Subject
				foundQuads[x].Predicate = in[x].Quad.Predicate
				foundQuads[x].Object = in[x].Quad.Object
				foundQuads[x].Label = in[x].Quad.Label

				// If the quad exists the Added[] will be non-empty
				if in[x].Action == graph.Add {
					foundQuads[x].Added = append(foundQuads[x].Added, in[x].ID.Int())
					quadCount += 1
				} else {
					foundQuads[x].Deleted = append(foundQuads[x].Deleted, in[x].ID.Int())
					quadCount -= 1
				}
			}
			_, err := tg.PutMulti(foundQuads[i : i+j])
			//qs.db.Context.Infof("updateQuads putmulti %#v", foundQuads[i:i+j])
			//_, err := datastore.PutMulti(c, keys[i:i+j], foundQuads)
			return err
		}, &datastore.TransactionOptions{XG: true})
		if err != nil {
			return 0, err
		}
	}
	return quadCount, nil
}

func (qs *QuadStore) updateMetadata(quadsAdded int64, nodesAdded int64) error {
	key := qs.createKeyForMetadata()
	foundMetadata := &MetadataEntry{Id: key.StringID(), _kind: key.Kind()}
	err := qs.db.RunInTransaction(func(tg *goon.Goon) error {
		err := tg.Get(foundMetadata)
		if err != nil && err != datastore.ErrNoSuchEntity {
			glog.Errorf("Error: %v", err)
			return err
		}
		foundMetadata.QuadCount += quadsAdded
		foundMetadata.NodeCount += nodesAdded
		_, err = tg.Put(foundMetadata)
		if err != nil {
			glog.Errorf("Error: %v", err)
		}
		return err
	}, nil)
	return err
}

func (qs *QuadStore) updateLog(in []graph.Delta) error {
	if qs.context == nil {
		err := errors.New("Error updating log, context is nil, graph not correctly initialised")
		return err
	}
	if len(in) == 0 {
		return errors.New("Nothing to log")
	}
	logEntries := make([]LogEntry, 0, len(in))
	for _, d := range in {
		var action string
		if d.Action == graph.Add {
			action = "Add"
		} else {
			action = "Delete"
		}

		entry := LogEntry{
			Id:        d.ID.Int(),
			LogID:     d.ID.Int(),
			Action:    action,
			Key:       qs.createKeyForQuad(d.Quad).String(),
			Timestamp: d.Timestamp.UnixNano(),
		}
		logEntries = append(logEntries, entry)
	}
	//qs.db.Context.Infof("logEntries %v %+v", len(logEntries), logEntries)
	_, err := qs.db.PutMulti(logEntries)
	if err != nil {
		glog.Errorf("Error updating log: %v", err)
	}
	return err
}

func (qs *QuadStore) QuadIterator(dir quad.Direction, v graph.Value) graph.Iterator {
	return NewIterator(qs, quadKind, dir, v)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, nodeKind)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, quadKind)
}

func (qs *QuadStore) ValueOf(s string) graph.Value {
	hasher := qs.makeHasher()
	id := qs.hashOf(s, hasher)
	return &Token{Kind: nodeKind, Hash: id}
}

func (qs *QuadStore) NameOf(val graph.Value) string {
	if qs.context == nil {
		glog.Error("Error in NameOf, context is nil, graph not correctly initialised")
		return ""
	}
	var key *datastore.Key
	if t, ok := val.(*Token); ok && t.Kind == nodeKind {
		key = qs.createKeyFromToken(t)
	} else {
		glog.Error("Token not valid")
		return ""
	}

	// TODO (stefankoshiw) implement a cache
	node := NodeEntry{Id: key.StringID()}
	err := qs.db.Get(&node)
	if err != nil {
		glog.Errorf("Error: %v", err)
		return ""
	}
	return node.Name
}

func (qs *QuadStore) Quad(val graph.Value) quad.Quad {
	if qs.context == nil {
		glog.Error("Error fetching quad, context is nil, graph not correctly initialised")
		return quad.Quad{}
	}
	var key *datastore.Key
	if t, ok := val.(*Token); ok && t.Kind == quadKind {
		key = qs.createKeyFromToken(t)
	} else {
		glog.Error("Token not valid")
		return quad.Quad{}
	}

	q := new(QuadEntry)
	q.Id = key.StringID()
	q.Kind = key.Kind()
	//err := datastore.Get(qs.context, key, q)
	err := qs.db.Get(q)
	if err != nil {
		// Red herring error : ErrFieldMismatch can happen when a quad exists but a field is empty
		if _, ok := err.(*datastore.ErrFieldMismatch); !ok {
			glog.Errorf("Error: %v", err)
		}
	}
	return quad.Quad{
		Subject:   q.Subject,
		Predicate: q.Predicate,
		Object:    q.Object,
		Label:     q.Label}
}

func (qs *QuadStore) Size() int64 {
	if qs.context == nil {
		glog.Error("Error fetching size, context is nil, graph not correctly initialised")
		return 0
	}
	//qs.context.Infof("Size %v", qs)

	key := qs.createKeyForMetadata()

	foundMetadata := new(MetadataEntry)
	foundMetadata.Id = key.StringID()
	foundMetadata._kind = key.Kind()
	//err := datastore.Get(qs.context, key, foundMetadata)
	err := qs.db.Get(foundMetadata)
	if err != nil {
		glog.Warningf("Error: %v", err)
		return 0
	}
	return foundMetadata.QuadCount
}

func (qs *QuadStore) NodeSize() int64 {
	if qs.context == nil {
		glog.Error("Error fetching node size, context is nil, graph not correctly initialised")
		return 0
	}
	//qs.context.Infof("NodeSize %v", qs)

	key := qs.createKeyForMetadata()
	foundMetadata := new(MetadataEntry)
	foundMetadata.Id = key.StringID()
	foundMetadata._kind = key.Kind()
	//err := datastore.Get(qs.context, key, foundMetadata)
	err := qs.db.Get(foundMetadata)
	if err != nil {
		glog.Warningf("Error: %v", err)
		return 0
	}
	return foundMetadata.NodeCount
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	if qs.context == nil {
		glog.Warning("Warning: HTTP Request context is nil, cannot get horizon from datastore.")
		return keys.NewSequentialKey(0)
	}
	qs.context.Infof("Horizon %v", qs)
	// Query log for last entry...
	q := datastore.NewQuery("logentry").Order("-Timestamp").Limit(1)
	var logEntries []LogEntry
	//_, err := q.GetAll(qs.context, &logEntries)
	_, err := qs.db.GetAll(q, &logEntries)
	if err != nil || len(logEntries) == 0 {
		// Error fetching horizon, probably graph is empty
		return keys.NewSequentialKey(0)
	}
	return keys.NewSequentialKey(logEntries[0].LogID)
}

func compareTokens(a, b graph.Value) bool {
	atok := a.(*Token)
	btok := b.(*Token)
	return atok.Kind == btok.Kind && atok.Hash == btok.Hash
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(compareTokens)
}

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return nil, false
}

func (qs *QuadStore) Close() {
	qs.context = nil
}

func (qs *QuadStore) QuadDirection(val graph.Value, dir quad.Direction) graph.Value {
	t, ok := val.(*Token)
	if !ok {
		glog.Error("Token not valid")
		return nil
	}
	if t.Kind == nodeKind {
		glog.Error("Node tokens not valid")
		return nil
	}
	var offset int
	switch dir {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (qs.hashSize * 2)
	case quad.Object:
		offset = (qs.hashSize * 2) * 2
	case quad.Label:
		offset = (qs.hashSize * 2) * 3
	}
	sub := t.Hash[offset : offset+(qs.hashSize*2)]
	return &Token{Kind: nodeKind, Hash: sub}
}

func (qs *QuadStore) hashOf(s string, hasher hash.Hash) string {
	hasher.Reset()
	key := make([]byte, 0, qs.hashSize)
	hasher.Write([]byte(s))
	key = hasher.Sum(key)
	return hex.EncodeToString(key)
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}
