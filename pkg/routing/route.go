package routing

import "strings"

const (
	routeSeparator    = ";"
	eventsPrefix      = "events:"
	eventsStorePrefix = "events_store:"
	queuesPrefix      = "queues:"
	routesPrefix      = "route:"
)

type Route struct {
	RoutesMap map[string][]string
}

func NewRoute() *Route {
	return &Route{
		RoutesMap: map[string][]string{"": []string{}, "events": []string{}, "events_store": []string{}, "queues": []string{}, "routes": []string{}},
	}
}

func (r *Route) Parse(channel string) *Route {
	segments := strings.Split(channel, routeSeparator)
	for i := 0; i < len(segments); i++ {
		destination, route := r.parseSegment(segments[i])
		if route == "" {
			continue
		}
		list, ok := r.RoutesMap[destination]
		if ok {
			if !r.hasRoute(list, route) {
				list = append(list, route)
				r.RoutesMap[destination] = list
			}
		}
	}

	return r
}

func (r *Route) HasRoutes() bool {
	return len(r.RoutesMap["routes"]) > 0
}

func (r *Route) parseSegment(seg string) (string, string) {
	if strings.HasPrefix(seg, eventsPrefix) {
		return "events", strings.TrimPrefix(seg, eventsPrefix)
	}
	if strings.HasPrefix(seg, eventsStorePrefix) {
		return "events_store", strings.TrimPrefix(seg, eventsStorePrefix)
	}
	if strings.HasPrefix(seg, queuesPrefix) {
		return "queues", strings.TrimPrefix(seg, queuesPrefix)
	}
	if strings.HasPrefix(seg, routesPrefix) {
		return "routes", strings.TrimPrefix(seg, routesPrefix)
	}
	return "", seg
}
func (r *Route) hasRoute(routeList []string, route string) bool {
	for _, listElement := range routeList {
		if route == listElement {
			return true
		}
	}
	return false
}
func (r *Route) Default(source string) string {
	blankList := r.RoutesMap[""]
	if len(blankList) > 0 {
		return blankList[0]
	}
	sourceList, ok := r.RoutesMap[source]
	if ok {
		if len(sourceList) > 0 {
			return sourceList[0]
		}
	}
	return ""
}

func IsRoutable(channel string) bool {
	if strings.Contains(channel, ";") || strings.Contains(channel, ":") {
		return true
	}
	return false
}
