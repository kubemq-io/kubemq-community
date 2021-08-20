package routing

import (
	"fmt"
	"regexp"
	"strings"
)

type RouteTable struct {
	routingTable []*RouteTableEntry
}

func newRouteTable(routingTable []*RouteTableEntry) *RouteTable {
	return &RouteTable{
		routingTable: routingTable,
	}
}

func (r *RouteTable) matchRegex(keyExp, key string) bool {
	regex, err := regexp.Compile(keyExp)
	if err != nil {
		return false
	}
	return regex.MatchString(key)

}
func (r *RouteTable) processTemplate(key, routes string) string {
	if !strings.Contains(routes, "{") {
		return routes
	}
	segments := strings.Split(key, ".")
	for i := 0; i < len(segments); i++ {
		routes = strings.Replace(routes, fmt.Sprintf("{%d}", i+1), segments[i], -1)
	}
	return routes
}
func (r *RouteTable) getRoutes(key string) []string {
	routesList := []string{}
	for _, entry := range r.routingTable {
		if r.matchRegex(entry.Key, key) {
			routesList = append(routesList, r.processTemplate(key, entry.Routes))
		}
	}
	return routesList
}
func (r *RouteTable) Parse(route *Route) *RouteTable {
	if !route.HasRoutes() {
		return r
	}
	for _, routeKey := range route.RoutesMap["routes"] {
		routing := r.getRoutes(routeKey)
		for _, channel := range routing {
			route.Parse(channel)
		}
	}
	return r
}
