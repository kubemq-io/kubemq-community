package metrics

import (
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/api"

	"strconv"
	"strings"
)

func getSnapshot(mf []*Family) *api.Snapshot {
	snapshot := api.NewSnapshot()
	system, stats := parseFamily(mf)
	channelsEntities, clientsEntities := makeEntities(stats)
	snapshot.SetSystem(system)
	snapshot.SetChannelEntities(channelsEntities).SetClientsEntities(clientsEntities)
	return snapshot
}

func parseFamily(mf []*Family) (*api.System, []*Stats) {
	si := api.NewSystem()
	serverState := config.GetServerState()
	if serverState != nil {
		si.SetVersion(serverState.Version)
	} else {
		si.SetVersion("Unknown")
	}
	var list []*Stats

	for _, family := range mf {
		switch family.Name {
		case "process_resident_memory_bytes":
			si.SetProcessMemory(getFloatValue(family.Metrics))
		case "go_memstats_alloc_bytes":
			si.SetProcessMemoryAllocation(getFloatValue(family.Metrics))
		case "process_cpu_seconds_total":
			si.SetTotalCPUSeconds(getFloatValue(family.Metrics))
		case "go_goroutines":
			si.SetGoRoutines(getInt64Value(family.Metrics))
		case "process_start_time_seconds":
			si.SetStartTime(getFloatValue(family.Metrics))
		case "go_threads":
			si.SetOSThreads(getInt64Value(family.Metrics))
		default:
			if strings.Contains(family.Name, "kubemq") {
				for _, metric := range family.Metrics {
					switch v := metric.(type) {
					case Metric:
						st := &Stats{
							Name:     strings.Replace(family.Name, "kubemq_", "", -1),
							Node:     v.Labels["node"],
							Type:     v.Labels["type"],
							Side:     v.Labels["side"],
							Channel:  v.Labels["channel"],
							ClientId: v.Labels["client_id"],
							Value:    v.Value,
						}
						list = append(list, st)
					}
				}
			}
		}
	}
	si.Calc()
	return si, list
}

func makeEntities(st []*Stats) (*api.EntitiesGroup, *api.EntitiesGroup) {
	channelsEntitiesGroup := api.NewEntitiesGroup()
	clientsEntitiesGroup := api.NewEntitiesGroup()
	for _, item := range st {
		channelType := item.Type
		channelName := item.Channel
		clientIdName := item.ClientId

		channelEntity, _ := channelsEntitiesGroup.GetEntity(channelType, channelName)
		if channelEntity == nil {
			channelEntity = api.NewEntity(channelType, channelName)
			channelsEntitiesGroup.AddEntity(channelType, channelEntity)
		}

		clientEntity, _ := clientsEntitiesGroup.GetEntity(fmt.Sprintf("%s/%s", channelType, channelName), clientIdName)
		if clientEntity == nil {
			clientEntity = api.NewEntity(fmt.Sprintf("%s/%s", channelType, channelName), clientIdName)
			clientsEntitiesGroup.AddEntity(fmt.Sprintf("%s/%s", channelType, channelName), clientEntity)
		}
		switch item.Kind() {
		case "messages_count":
			channelEntity.SetValues(item.Side, "messages", int64(item.Float64()))
			clientEntity.SetValues(item.Side, "messages", int64(item.Float64()))
		case "messages_volume":
			channelEntity.SetValues(item.Side, "volume", int64(item.Float64()))
			clientEntity.SetValues(item.Side, "volume", int64(item.Float64()))
		case "errors_count":
			channelEntity.SetValues(item.Side, "errors", int64(item.Float64()))
			clientEntity.SetValues(item.Side, "errors", int64(item.Float64()))
		case "messages_expired":
			channelEntity.SetValues(item.Side, "expired", int64(item.Float64()))
			clientEntity.SetValues(item.Side, "expired", int64(item.Float64()))
		case "messages_delayed":
			channelEntity.SetValues(item.Side, "delayed", int64(item.Float64()))
			clientEntity.SetValues(item.Side, "delayed", int64(item.Float64()))
		case "last_seen":
			channelEntity.SetValues(item.Side, "last_seen", int64(item.Float64()))
			clientEntity.SetValues(item.Side, "last_seen", int64(item.Float64()))
		}
		channelEntity.SetClient(item.Side, item.ClientId)

	}
	channelsEntitiesGroup.ReCalcLastSeen()
	clientsEntitiesGroup.ReCalcLastSeen()
	return channelsEntitiesGroup, clientsEntitiesGroup
}

func getInt64Value(metrics []interface{}) int64 {
	if len(metrics) == 1 {
		value, ok := metrics[0].(Metric)
		if ok {
			val, err := strconv.ParseInt(value.Value, 10, 64)
			if err != nil {
				return -1
			} else {
				return val
			}
		}
	}
	return 0
}
func getFloatValue(metrics []interface{}) float64 {
	if len(metrics) == 1 {
		value, ok := metrics[0].(Metric)
		if ok {
			val, err := strconv.ParseFloat(value.Value, 64)
			if err != nil {
				return 0
			} else {
				return val
			}
		}
	}
	return 0
}
