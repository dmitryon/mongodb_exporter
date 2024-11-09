// mongodb_exporter
// Copyright (C) 2017 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exporter

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type collstatsCollector struct {
	ctx  context.Context
	base *baseCollector

	compatibleMode  bool
	discoveringMode bool
	topologyInfo    labelsGetter

	collections []string

	documentDbCompatible bool
}

// newCollectionStatsCollector creates a collector for statistics about collections.
func newCollectionStatsCollector(ctx context.Context, client *mongo.Client, logger *logrus.Logger, compatible, discovery bool, topology labelsGetter, collections []string, documentDbCompatible bool) *collstatsCollector {
	return &collstatsCollector{
		ctx:  ctx,
		base: newBaseCollector(client, logger.WithFields(logrus.Fields{"collector": "collstats"})),

		compatibleMode:  compatible,
		discoveringMode: discovery,
		topologyInfo:    topology,

		collections: collections,

		documentDbCompatible: documentDbCompatible,
	}
}

func (d *collstatsCollector) Describe(ch chan<- *prometheus.Desc) {
	d.base.Describe(d.ctx, ch, d.collect)
}

func (d *collstatsCollector) Collect(ch chan<- prometheus.Metric) {
	d.base.Collect(ch)
}

func (d *collstatsCollector) collect(ch chan<- prometheus.Metric) {
	defer measureCollectTime(ch, "mongodb", "collstats")()

	client := d.base.client
	logger := d.base.logger

	var collections []string
	if d.discoveringMode {
		onlyCollectionsNamespaces, err := listAllCollections(d.ctx, client, d.collections, systemDBs, true)
		if err != nil {
			logger.Errorf("cannot auto discover databases and collections: %s", err.Error())

			return
		}

		collections = fromMapToSlice(onlyCollectionsNamespaces)
	} else {
		var err error
		collections, err = checkNamespacesForViews(d.ctx, client, d.collections)
		if err != nil {
			logger.Errorf("cannot list collections: %s", err.Error())
			return
		}
	}

	for _, dbCollection := range collections {
		parts := strings.Split(dbCollection, ".")
		if len(parts) < 2 { //nolint:gomnd
			continue
		}

		database := parts[0]
		collection := strings.Join(parts[1:], ".") // support collections having a .

		// exclude system collections
		if strings.HasPrefix(collection, "system.") {
			continue
		}

		var stats []bson.M
		if d.documentDbCompatible {
			db := client.Database(database)
			result := db.RunCommand(d.ctx, bson.M{"collStats": collection})
			if result.Err() != nil {
				logger.Errorf("cannot execute collStats for collection %s.%s: %s", database, collection, result.Err())

				continue
			}

			var doc bson.M
			if result.Decode(&doc) != nil {
				logger.Errorf("cannot decode collStats for collection %s.%s: %s", database, collection, result.Err())

				continue
			}

			mongodbCompatibleCollStats := removeNilValues(bson.M{
				"ns": doc["ns"],
				"storageStats": bson.M{
					"avgObjSize":     doc["avgObjSize"],
					"count":          doc["count"],
					"size":           doc["size"],
					"storageSize":    doc["storageSize"],
					"capped":         doc["capped"],
					"nindexes":       doc["nindexes"],
					"totalIndexSize": doc["totalIndexSize"],
					"indexSizes":     doc["indexSizes"],
				},
				"compression": doc["compression"],
				"collScans":   doc["collScans"],
				"idxScans":    doc["idxScans"],
				"opCounter":   doc["opCounter"],
				"cacheStats":  doc["cacheStats"],
			})

			stats = append(
				stats,
				mongodbCompatibleCollStats,
			)

			logger.Debugf("collStats metrics for %s.%s", database, collection)
		} else {
			aggregation := bson.D{
				{
					Key: "$collStats", Value: bson.M{
						// TODO: PMM-9568 : Add support to handle histogram metrics
						"latencyStats": bson.M{"histograms": false},
						"storageStats": bson.M{"scale": 1},
					},
				},
			}
			project := bson.D{
				{
					Key: "$project", Value: bson.M{
						"storageStats.wiredTiger":   0,
						"storageStats.indexDetails": 0,
					},
				},
			}
			cursor, err := client.Database(database).Collection(collection).Aggregate(d.ctx, mongo.Pipeline{aggregation, project})
			if err != nil {
				logger.Errorf("cannot get $collstats cursor for collection %s.%s: %s", database, collection, err)

				continue
			}

			if err = cursor.All(d.ctx, &stats); err != nil {
				logger.Errorf("cannot get $collstats for collection %s.%s: %s", database, collection, err)

				continue
			}

			logger.Debugf("$collStats metrics for %s.%s", database, collection)
		}
		debugResult(logger, stats)

		prefix := "collstats"
		labels := d.topologyInfo.baseLabels()
		labels["database"] = database
		labels["collection"] = collection

		for _, metrics := range stats {
			if shard, ok := metrics["shard"].(string); ok {
				labels["shard"] = shard
			}

			for _, metric := range makeMetrics(prefix, metrics, labels, d.compatibleMode) {
				ch <- metric
			}
		}
	}
}

func removeNilValues(m bson.M) bson.M {
	filteredMap := make(bson.M)
	for k, v := range m {
		if v == nil {
			continue
		}

		// process non-nil value or nested map
		if m1, ok := v.(bson.M); ok {
			filteredMap[k] = removeNilValues(m1)
		} else {
			filteredMap[k] = v
		}
	}
	return filteredMap
}

var _ prometheus.Collector = (*collstatsCollector)(nil)
