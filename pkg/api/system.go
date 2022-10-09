package api

import (
	"math"
	"os"
	"runtime"
	"time"
)

func getHostname() string {
	host, _ := os.Hostname()
	return host
}

type System struct {
	Hostname                string  `json:"hostname"`
	Version                 string  `json:"version"`
	ProcessMemory           float64 `json:"process_memory"`
	ProcessMemoryAllocation float64 `json:"process_memory_allocation"`
	GoRoutines              int64   `json:"go_routines"`
	OSThreads               int64   `json:"os_threads"`
	TotalCPUSeconds         float64 `json:"total_cpu_seconds"`
	TotalCPUs               int     `json:"total_cpus"`
	StartTime               float64 `json:"start_time"`
	Uptime                  float64 `json:"uptime"`
	CPUUtilization          float64 `json:"cpu_utilization"`
	MemoryUtilization       float64 `json:"memory_utilization"`
	UpdatedAt               int64   `json:"updatedAt"`
	ActiveClients           int     `json:"activeClients"`
}

func NewSystem() *System {
	return &System{
		Hostname:  getHostname(),
		TotalCPUs: runtime.NumCPU(),
	}
}
func (s *System) SetVersion(value string) *System {
	s.Version = value
	return s
}
func (s *System) SetProcessMemory(value float64) *System {
	s.ProcessMemory = math.Round((value)*100) / 100
	return s
}
func (s *System) SetProcessMemoryAllocation(value float64) *System {
	s.ProcessMemoryAllocation = math.Round((value)*100) / 100
	return s
}

func (s *System) SetGoRoutines(value int64) *System {
	s.GoRoutines = value
	return s
}

func (s *System) SetOSThreads(value int64) *System {
	s.OSThreads = value
	return s
}
func (s *System) SetStartTime(value float64) *System {
	s.StartTime = value
	return s
}

func (s *System) SetTotalCPUSeconds(value float64) *System {
	s.TotalCPUSeconds = math.Round(value*100) / 100
	return s
}
func (s *System) SetCPUUtilization(lastUptime, lastCpuSeconds float64) *System {
	uptimeDiff := s.Uptime - lastUptime
	totalUptime := uptimeDiff * float64(s.TotalCPUs)
	if totalUptime == 0 {
		totalUptime = 1
	}
	cpuSecDiff := s.TotalCPUSeconds - lastCpuSeconds
	cpuUtil := (cpuSecDiff / totalUptime) * 100
	s.CPUUtilization = math.Round(cpuUtil*100) / 100
	return s
}
func (s *System) Calc() *System {
	s.Uptime = float64(time.Now().Unix()) - s.StartTime
	memUtilL := (s.ProcessMemoryAllocation / s.ProcessMemory) * 100
	s.MemoryUtilization = math.Round(memUtilL*100) / 100
	return s
}

func (s *System) SetUpdatedAt(value int64) *System {
	s.UpdatedAt = value
	return s
}

func (s *System) SetActiveClients(value int) *System {
	s.ActiveClients = value
	return s
}
