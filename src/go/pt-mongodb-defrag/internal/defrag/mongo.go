// This program is copyright 2026 Percona LLC and/or its affiliates.
//
// THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation, version 2.
//
// You should have received a copy of the GNU General Public License, version 2
// along with this program; if not, see <https://www.gnu.org/licenses/>.

package defrag

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	client *mongo.Client
}

func Connect(ctx context.Context, uri string) (*Mongo, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return &Mongo{client: client}, nil
}

func (m *Mongo) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func (m *Mongo) BuildInfo(ctx context.Context) (BuildInfo, error) {
	var info BuildInfo
	err := m.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&info)
	return info, err
}

func (m *Mongo) EnsureMongos(ctx context.Context) error {
	var result struct {
		IsDBGrid any `bson:"isdbgrid"`
		Msg      any `bson:"msg"`
		OK       any `bson:"ok"`
	}
	if err := m.client.Database("admin").RunCommand(ctx, bson.D{{Key: "isdbgrid", Value: 1}}).Decode(&result); err != nil {
		return err
	}
	return nil
}

func (m *Mongo) CollectionMetadata(ctx context.Context, ns string) (CollectionMetadata, error) {
	var meta CollectionMetadata
	err := m.client.Database("config").Collection("collections").FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Decode(&meta)
	return meta, err
}

func (m *Mongo) ClusterChunkSizeMB(ctx context.Context) (float64, error) {
	var doc struct {
		Value any `bson:"value"`
	}
	err := m.client.Database("config").Collection("settings").FindOne(ctx, bson.D{{Key: "_id", Value: "chunksize"}}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return 64, nil
		}
		return 0, err
	}

	switch v := doc.Value.(type) {
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("unsupported chunksize type %T", doc.Value)
	}
}

func (m *Mongo) HasZones(ctx context.Context, ns string) (bool, error) {
	count, err := m.client.Database("config").Collection("tags").CountDocuments(ctx, bson.D{{Key: "ns", Value: ns}})
	return count > 0, err
}

func (m *Mongo) Chunks(ctx context.Context, meta CollectionMetadata, ns string) ([]Chunk, error) {
	coll := m.client.Database("config").Collection("chunks")
	opts := options.Find().SetSort(bson.D{{Key: "min", Value: 1}})

	cur, err := coll.Find(ctx, bson.D{{Key: "uuid", Value: meta.UUID}}, opts)
	if err != nil {
		return nil, err
	}

	var chunks []Chunk
	if err := cur.All(ctx, &chunks); err != nil {
		return nil, err
	}
	_ = cur.Close(ctx)

	if len(chunks) == 0 {
		cur, err = coll.Find(ctx, bson.D{{Key: "ns", Value: ns}}, opts)
		if err != nil {
			return nil, err
		}
		defer cur.Close(ctx)
		if err := cur.All(ctx, &chunks); err != nil {
			return nil, err
		}
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks found for %s", ns)
	}
	return chunks, nil
}

func (m *Mongo) DataSize(ctx context.Context, dbName, ns string, keyPattern, min, max bson.D) (ChunkMetrics, error) {
	cmd := bson.D{
		{Key: "dataSize", Value: ns},
		{Key: "keyPattern", Value: keyPattern},
		{Key: "min", Value: min},
		{Key: "max", Value: max},
		{Key: "estimate", Value: false},
	}
	var res struct {
		Size       int64 `bson:"size"`
		NumObjects int64 `bson:"numObjects"`
		Millis     int64 `bson:"millis"`
	}
	err := m.client.Database(dbName).RunCommand(ctx, cmd).Decode(&res)
	if err != nil {
		return ChunkMetrics{}, err
	}
	return ChunkMetrics{Bytes: res.Size, Documents: res.NumObjects, Millis: res.Millis}, nil
}

func (m *Mongo) MoveRange(ctx context.Context, ns string, min, max bson.D, toShard string) error {
	cmd := bson.D{
		{Key: "moveRange", Value: ns},
		{Key: "min", Value: min},
		{Key: "max", Value: max},
		{Key: "toShard", Value: toShard},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

func (m *Mongo) MergeChunks(ctx context.Context, ns string, min, max bson.D) error {
	cmd := bson.D{
		{Key: "mergeChunks", Value: ns},
		{Key: "bounds", Value: bson.A{min, max}},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

func (m *Mongo) ClearJumboFlag(ctx context.Context, ns string, min, max bson.D) error {
	cmd := bson.D{
		{Key: "clearJumboFlag", Value: ns},
		{Key: "bounds", Value: bson.A{min, max}},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

func (m *Mongo) SplitChunk(ctx context.Context, ns string, chunk Chunk, hashed bool) error {
	var cmd bson.D
	if hashed {
		cmd = bson.D{
			{Key: "split", Value: ns},
			{Key: "bounds", Value: bson.A{chunk.Min, chunk.Max}},
		}
	} else {
		cmd = bson.D{
			{Key: "split", Value: ns},
			{Key: "find", Value: chunk.Min},
		}
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

// ChunkByID looks up a single chunk in config.chunks by _id.
func (m *Mongo) ChunkByID(ctx context.Context, ns, chunkID string) (chunkDocument, CollectionMetadata, error) {
	meta, err := m.CollectionMetadata(ctx, ns)
	if err != nil {
		return chunkDocument{}, CollectionMetadata{}, fmt.Errorf("load collection metadata: %w", err)
	}

	var doc chunkDocument
	err = m.client.Database("config").Collection("chunks").
		FindOne(ctx, bson.D{{Key: "_id", Value: chunkID}}).
		Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return chunkDocument{}, CollectionMetadata{}, fmt.Errorf("chunk %q not found in config.chunks", chunkID)
		}
		return chunkDocument{}, CollectionMetadata{}, fmt.Errorf("lookup chunk %q: %w", chunkID, err)
	}

	return doc, meta, nil
}

// AutoMergerIntervalSecs reads the cluster's autoMergerIntervalSecs parameter.
// Returns the MongoDB default (10) if the parameter cannot be read.
func (m *Mongo) AutoMergerIntervalSecs(ctx context.Context) (int64, error) {
	var result struct {
		AutoMergerIntervalSecs int64 `bson:"autoMergerIntervalSecs"`
		OK                     int   `bson:"ok"`
	}
	err := m.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "getParameter", Value: 1},
		{Key: "autoMergerIntervalSecs", Value: 1},
	}).Decode(&result)
	if err != nil || result.OK != 1 {
		return 10, nil // default fallback
	}
	return result.AutoMergerIntervalSecs, nil
}

// ConfigureAutoMerger enables or disables the AutoMerger for a collection.
func (m *Mongo) ConfigureAutoMerger(ctx context.Context, ns string, enabled bool) error {
	cmd := bson.D{
		{Key: "configureCollectionBalancing", Value: ns},
		{Key: "autoMerger", Value: enabled},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

// MoveRangeWithFind moves a chunk identified by a find query to a target shard.
// forceJumbo bypasses the document-count move restriction.
func (m *Mongo) MoveRangeWithFind(ctx context.Context, ns string, find bson.D, toShard string, forceJumbo bool) error {
	cmd := bson.D{
		{Key: "moveRange", Value: ns},
		{Key: "find", Value: find},
		{Key: "toShard", Value: toShard},
	}
	if forceJumbo {
		cmd = append(cmd, bson.E{Key: "forceJumbo", Value: true})
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

func mustMarshalBounds(doc bson.D) string {
	data, _ := bson.MarshalExtJSON(doc, true, false)
	return string(data)
}
