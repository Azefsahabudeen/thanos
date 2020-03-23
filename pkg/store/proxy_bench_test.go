// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/storetestutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestProxySeries(t *testing.T) {
	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchProxySeries(t, samplesPerSeries, series)
	})
}

func BenchmarkProxySeries(b *testing.B) {
	tb := testutil.NewTB(b)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchProxySeries(t, samplesPerSeries, series)
	})
}

func benchProxySeries(t testutil.TB, totalSamples, totalSeries int) {
	const numOfClients = 4

	samplesPerSeriesPerClient := totalSamples / numOfClients
	if samplesPerSeriesPerClient == 0 {
		samplesPerSeriesPerClient = 1
	}
	seriesPerClient := totalSeries / numOfClients
	if seriesPerClient == 0 {
		seriesPerClient = 1
	}

	random := rand.New(rand.NewSource(120))
	clients := make([]Client, numOfClients)
	for j := range clients {
		var resps []*storepb.SeriesResponse

		head, created := storetestutil.CreateHeadWithSeries(t, j, samplesPerSeriesPerClient, seriesPerClient, random)
		testutil.Ok(t, head.Close())

		for i := 0; i < len(created); i++ {
			resps = append(resps, storepb.NewSeriesResponse(&created[i]))
		}

		clients[j] = &testClient{
			StoreClient: &mockedStoreAPI{
				RespSeries: resps,
			},
			minTime: math.MinInt64,
			maxTime: math.MaxInt64,
		}
	}

	logger := log.NewNopLogger()
	store := &ProxyStore{
		logger:          logger,
		stores:          func() []Client { return clients },
		metrics:         newProxyStoreMetrics(nil),
		responseTimeout: 0,
	}

	var allResps []*storepb.SeriesResponse
	var expected []storepb.Series
	lastLabels := storepb.Series{}
	for _, c := range clients {
		m := c.(*testClient).StoreClient.(*mockedStoreAPI)

		for _, r := range m.RespSeries {
			allResps = append(allResps, r)

			// Proxy will merge all series with same labels without limit (https://github.com/thanos-io/thanos/issues/2332).
			// Let's do this here as well.
			x := storepb.Series{Labels: r.GetSeries().Labels}
			if x.String() == lastLabels.String() {
				expected[len(expected)-1].Chunks = append(expected[len(expected)-1].Chunks, r.GetSeries().Chunks...)
				continue
			}
			lastLabels = x
			expected = append(expected, *r.GetSeries())
		}

	}

	chunkLen := len(allResps[len(allResps)-1].GetSeries().Chunks)
	maxTime := allResps[len(allResps)-1].GetSeries().Chunks[chunkLen-1].MaxTime
	storetestutil.TestServerSeries(t, store,
		&storetestutil.SeriesCase{
			Name: fmt.Sprintf("%d client with %d samples, %d series each", numOfClients, samplesPerSeriesPerClient, seriesPerClient),
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: maxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			ExpectedSeries: expected,
		},
	)

	// Change client to just one.
	store.stores = func() []Client {
		return []Client{&testClient{
			StoreClient: &mockedStoreAPI{
				// All responses.
				RespSeries: allResps,
			},
			labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
			minTime:   math.MinInt64,
			maxTime:   math.MaxInt64,
		}}
	}

	// In this we expect exactly the same response as input.
	expected = expected[:0]
	for _, r := range allResps {
		expected = append(expected, *r.GetSeries())
	}
	storetestutil.TestServerSeries(t, store,
		&storetestutil.SeriesCase{
			Name: fmt.Sprintf("single client with %d samples, %d series", totalSamples, totalSeries),
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: maxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			ExpectedSeries: expected,
		},
	)
}
