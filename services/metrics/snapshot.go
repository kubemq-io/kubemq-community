package metrics

import (
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/api"

	"strconv"
	"strings"
)

func getSnapshot(mf []*Family) *api.Snapshot {
	snapshot := api.NewSnapshot()
	system, stats := parseFamily(mf)
	entities := makeEntities(stats)
	snapshot.SetSystem(system)
	snapshot.SetEntities(entities)
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

func makeEntities(st []*Stats) *api.EntitiesGroup {
	entitiesGroup := api.NewEntitiesGroup()
	for _, item := range st {
		family := item.Type
		name := item.Channel

		entity, _ := entitiesGroup.GetEntity(family, name)
		if entity == nil {
			entity = api.NewEntity(family, name)
			entitiesGroup.AddEntity(family, entity)
		}
		switch item.Kind() {
		case "messages_count":
			entity.SetValues(item.Side, "messages", int64(item.Float64()))
		case "messages_volume":
			entity.SetValues(item.Side, "volume", int64(item.Float64()))
		case "errors_count":
			entity.SetValues(item.Side, "errors", int64(item.Float64()))
		case "last_seen":
			entity.SetValues(item.Side, "last_seen", int64(item.Float64()))
		}
		entity.SetClient(item.Side, item.ClientId)
	}
	entitiesGroup.ReCalcLastSeen()
	return entitiesGroup
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
