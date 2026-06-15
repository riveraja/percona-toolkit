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
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	client            *mongo.Client
	metadataTimeout   time.Duration
	commandTimeout    time.Duration
}

func Connect(ctx context.Context, uri string, metadataTimeout, commandTimeout time.Duration) (*Mongo, error) {
	if metadataTimeout <= 0 {
		metadataTimeout = 30 * time.Second
	}
	if commandTimeout <= 0 {
		commandTimeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		disconnectErr := client.Disconnect(context.Background())
		if disconnectErr != nil {
			return nil, fmt.Errorf("ping failed: %w; also failed to disconnect: %v", err, disconnectErr)
		}
		return nil, fmt.Errorf("ping failed: %w", err)
	}
	return &Mongo{
		client:          client,
		metadataTimeout: metadataTimeout,
		commandTimeout:  commandTimeout,
	}, nil
}

func (m *Mongo) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func (m *Mongo) BuildInfo(ctx context.Context) (BuildInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, m.metadataTimeout)
	defer cancel()
	var info BuildInfo
	err := m.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&info)
	return info, err
}

func (m *Mongo) EnsureMongos(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.metadataTimeout)
	defer cancel()

	var result bson.M
	if err := m.client.Database("admin").RunCommand(ctx, bson.D{{Key: "isdbgrid", Value: 1}}).Decode(&result); err != nil {
		return err
	}
	if okRaw, exists := result["ok"]; exists && !isCommandOK(okRaw) {
		return fmt.Errorf("isdbgrid command failed: %v", result)
	}
	if !isMongosResult(result) {
		return fmt.Errorf("connection target is not mongos: %v", result)
	}
	return nil
}

func (m *Mongo) CollectionMetadata(ctx context.Context, ns string) (CollectionMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, m.metadataTimeout)
	defer cancel()
	var meta CollectionMetadata
	err := m.client.Database("config").Collection("collections").FindOne(ctx, bson.D{{Key: "_id", Value: ns}}).Decode(&meta)
	return meta, err
}

func (m *Mongo) ClusterChunkSizeMB(ctx context.Context) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, m.metadataTimeout)
	defer cancel()
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
	ctx, cancel := context.WithTimeout(ctx, m.metadataTimeout)
	defer cancel()
	count, err := m.client.Database("config").Collection("tags").CountDocuments(ctx, bson.D{{Key: "ns", Value: ns}})
	return count > 0, err
}

func (m *Mongo) Chunks(ctx context.Context, meta CollectionMetadata, ns string) ([]Chunk, error) {
	ctx, cancel := context.WithTimeout(ctx, m.commandTimeout)
	defer cancel()
	coll := m.client.Database("config").Collection("chunks")
	opts := options.Find().
		SetSort(bson.D{{Key: "min", Value: 1}}).
		SetProjection(bson.D{
			{Key: "min", Value: 1},
			{Key: "max", Value: 1},
			{Key: "shard", Value: 1},
			{Key: "jumbo", Value: 1},
		})

	chunks, err := loadChunks(ctx, coll, bson.D{{Key: "uuid", Value: meta.UUID}}, opts)
	if err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		chunks, err = loadChunks(ctx, coll, bson.D{{Key: "ns", Value: ns}}, opts)
		if err != nil {
			return nil, err
		}
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks found for %s", ns)
	}
	return chunks, nil
}

func loadChunks(ctx context.Context, coll *mongo.Collection, filter interface{}, opts *options.FindOptions) ([]Chunk, error) {
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	chunks := make([]Chunk, 0, 1024)
	for cur.Next(ctx) {
		var chunk Chunk
		if err := cur.Decode(&chunk); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (m *Mongo) DataSize(ctx context.Context, dbName, ns string, keyPattern, min, max bson.D, estimate bool) (ChunkMetrics, error) {
	ctx, cancel := context.WithTimeout(ctx, m.commandTimeout)
	defer cancel()
	cmd := bson.D{
		{Key: "dataSize", Value: ns},
		{Key: "keyPattern", Value: keyPattern},
		{Key: "min", Value: min},
		{Key: "max", Value: max},
		{Key: "estimate", Value: estimate},
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
	return ChunkMetrics{Bytes: res.Size, Documents: res.NumObjects, Millis: res.Millis, Estimated: estimate}, nil
}

func (m *Mongo) MoveRange(ctx context.Context, ns string, min, max bson.D, toShard string) error {
	ctx, cancel := context.WithTimeout(ctx, m.commandTimeout)
	defer cancel()
	cmd := bson.D{
		{Key: "moveRange", Value: ns},
		{Key: "min", Value: min},
		{Key: "max", Value: max},
		{Key: "toShard", Value: toShard},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

func (m *Mongo) MergeChunks(ctx context.Context, ns string, min, max bson.D) error {
	ctx, cancel := context.WithTimeout(ctx, m.commandTimeout)
	defer cancel()
	cmd := bson.D{
		{Key: "mergeChunks", Value: ns},
		{Key: "bounds", Value: bson.A{min, max}},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

func (m *Mongo) ClearJumboFlag(ctx context.Context, ns string, min, max bson.D) error {
	ctx, cancel := context.WithTimeout(ctx, m.commandTimeout)
	defer cancel()
	cmd := bson.D{
		{Key: "clearJumboFlag", Value: ns},
		{Key: "bounds", Value: bson.A{min, max}},
	}
	return m.client.Database("admin").RunCommand(ctx, cmd).Err()
}

func (m *Mongo) SplitChunk(ctx context.Context, ns string, chunk Chunk, hashed bool) error {
	ctx, cancel := context.WithTimeout(ctx, m.commandTimeout)
	defer cancel()
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

func marshalBounds(doc bson.D) (string, error) {
	data, err := bson.MarshalExtJSON(doc, true, false)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func isCommandOK(v any) bool {
	switch t := v.(type) {
	case int32:
		return t == 1
	case int64:
		return t == 1
	case float64:
		return t == 1
	case bool:
		return t
	default:
		return false
	}
}

func isMongosResult(result bson.M) bool {
	if _, ok := result["isdbgrid"]; ok {
		return true
	}
	if msg, ok := result["msg"].(string); ok && strings.EqualFold(msg, "isdbgrid") {
		return true
	}
	return false
}
