package config

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ghodss/yaml"

	"strings"

	"github.com/spf13/viper"
)

var appConfig *Config
var once sync.Once
var configFile = "./config.yaml"

type ServerState struct {
	Host                string `json:"host"`
	Version             string `json:"version"`
	ServerStartTime     int64  `json:"server_start_time"`
	ServerUpTimeSeconds int64  `json:"server_up_time_seconds"`
}

func GetServerState() *ServerState {
	if appConfig == nil {
		return &ServerState{}
	}
	return &ServerState{
		Host:                appConfig.Host,
		Version:             appConfig.version,
		ServerStartTime:     appConfig.startServerTime,
		ServerUpTimeSeconds: time.Now().Unix() - appConfig.startServerTime,
	}
}
func Host() string {
	if appConfig != nil {
		return appConfig.Host
	} else {
		return GetHostname()
	}
}

type Config struct {
	Host            string                `json:"-"`
	Client          *ClientConfig         `json:"client"`
	Api             *ApiConfig            `json:"api"`
	Log             *LogConfig            `json:"log"`
	Broker          *BrokerConfig         `json:"broker"`
	Store           *StoreConfig          `json:"store"`
	Queue           *QueueConfig          `json:"queue"`
	Grpc            *GrpcConfig           `json:"grpc"`
	Rest            *RestConfig           `json:"rest"`
	Authorization   *AuthorizationConfig  `json:"authorization"`
	Authentication  *AuthenticationConfig `json:"authentication"`
	Security        *SecurityConfig       `json:"security"`
	Routing         *RoutingConfig        `json:"routing"`
	startServerTime int64
	version         string
}

func (c *Config) SetVersion(value string) *Config {
	c.version = value
	return c
}

func (c *Config) GetVersion() string {
	return c.version
}

func GetHostname() string {
	hostname, _ := os.Hostname()
	if strings.Contains("0123456789", hostname[:1]) {
		hostname = "kubemq-" + hostname
	}
	hostname = strings.Replace(hostname, ".", "_", 100)
	return hostname
}

func GetConfig() *Config {
	if appConfig == nil {
		return GetDefaultConfig()
	}
	return appConfig
}

func GetDefaultConfig() *Config {
	bindViperEnv(
		"Key",
		"License",
		"LicenseFileName",
	)
	return &Config{
		Host:            "",
		Client:          defaultClientConfig(),
		Api:             defaultApiConfig(),
		Log:             defaultLogConfig(),
		Broker:          defaultBrokerConfig(),
		Store:           defaultStoreConfig(),
		Queue:           defaultQueueConfig(),
		Grpc:            defaultGRPCConfig(),
		Rest:            defaultRestConfig(),
		Authorization:   defaultAuthorizationConfig(),
		Authentication:  defaultAuthenticationConfig(),
		Security:        defaultSecurityConfig(),
		Routing:         defaultRoutingConfig(),
		startServerTime: 0,
		version:         "",
	}
}
func getConfigFormat(in []byte) string {
	c := &Config{}
	err := yaml.Unmarshal(in, c)
	if err == nil {
		return ".yaml"
	}
	err = toml.Unmarshal(in, c)
	if err == nil {
		return ".toml"
	}
	return ""
}

func decodeBase64(in string) (string, error) {
	// base64 string cannot contain space so this is indication of base64 string
	if !strings.Contains(in, " ") {
		sDec, err := base64.StdEncoding.DecodeString(in)
		if err != nil {
			return "", fmt.Errorf("error decoding config file base64 string: %s ", err.Error())
		}
		return string(sDec), nil
	}
	return "", fmt.Errorf("invalid base64 string")
}

// Attempt to find config file and return absolute path
func findAbsConfigPath(path string) (string, error) {
	// Check to see if yaml or toml
	fileExt := filepath.Ext(path)
	if fileExt != ".yaml" && fileExt != ".toml" {
		return "", fmt.Errorf("invalid file format '%s'", fileExt)
	}

	// next check if it's already absolute path
	if filepath.IsAbs(path) {
		return path, nil
	}

	// TODO: Currently this tries to load from exec path then CWD to mimmic the previous behavior
	// Consider switching the order to CWD then exec path

	// Try to find the file relative to the executable path
	execPath, err := os.Executable()
	if err == nil {
		execPath = filepath.Join(filepath.Dir(execPath), path)
		_, statErr := os.Stat(execPath)
		if statErr == nil {
			return execPath, nil
		}
	}

	// Get the absolute path using the current directory
	cwdPath, err := filepath.Abs(path)
	_, statErr := os.Stat(cwdPath)
	if err == nil && statErr == nil {
		return cwdPath, nil
	}

	return "", fmt.Errorf("config file not found in exec path '%s' or cwd '%s'", execPath, cwdPath)
}

func validateConfigFormat(path string) bool {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return false
	}

	fileExt := getConfigFormat(data)
	if fileExt == "" {
		return false
	}

	return strings.HasSuffix(path, fileExt)
}

func getConfigDataFromEnv() (string, error) {
	envConfig, ok := os.LookupEnv("CONFIG")
	if ok {
		// Try to decode base64 string
		envConfigData, err := decodeBase64(envConfig)
		if err == nil {
			fileExt := getConfigFormat([]byte(envConfigData))
			if fileExt == "" {
				return "", fmt.Errorf("invalid environment config format")
			}
			confFile, err := filepath.Abs("./config" + fileExt)
			if err != nil {
				return "", fmt.Errorf("error getting absolute path for config: %s", err.Error())
			}
			err = ioutil.WriteFile(confFile, []byte(envConfigData), 0600)
			if err != nil {
				return "", fmt.Errorf("cannot save environment config file")
			}
			return confFile, nil
		}

		// Try to use config as a path
		absPath, err := getConfigDataFromLocalFile(envConfig)
		if err != nil {
			return "", fmt.Errorf("cannot convert '%s' to absolute path: %s", envConfig, err.Error())
		}
		return absPath, nil
	}
	return "", fmt.Errorf("no config data from environment variable")
}

func getConfigDataFromLocalFile(filename string) (string, error) {
	absPath, err := findAbsConfigPath(filename)
	if err != nil {
		return "", fmt.Errorf("error getting absolute path for '%s': %s", filename, err.Error())
	}

	if !validateConfigFormat(absPath) {
		return "", fmt.Errorf("invalid config format")
	}
	return absPath, nil
}

// Gets the absolute path for the config file
func getConfigFile() (string, error) {
	// Attempt to get config file from env
	loadedConfigFile, err := getConfigDataFromEnv()
	if err == nil {
		return loadedConfigFile, nil
	}

	// Get config from local file, checking executing directory then CWD
	loadedConfigFile, err = getConfigDataFromLocalFile(configFile)
	if err != nil {
		return "", err
	}

	return loadedConfigFile, nil
}

func getConfigRecord(paths ...string) *Config {
	appConfig := GetDefaultConfig()
	appConfig.startServerTime = time.Now().Unix()
	// TODO: Should we not check these paths like we do the config loaded from configFile?
	for _, paths := range paths {
		viper.AddConfigPath(paths)
	}

	loadedConfigFile, err := getConfigFile()
	if err == nil {
		viper.SetConfigFile(loadedConfigFile)
	}
	_ = viper.BindEnv("Host", "HOST")
	_ = viper.ReadInConfig()
	err = viper.Unmarshal(appConfig)
	if err != nil {
		log.Println(fmt.Sprintf("error loading configuration: %s, using defaults", err))
		appConfig = GetDefaultConfig()
	}
	if appConfig.Host == "" {
		appConfig.Host = GetHostname()
	}
	d, _ := yaml.Marshal(appConfig)
	_ = ioutil.WriteFile(configFile, d, 0600)
	return appConfig
}

func (c *Config) Validate() error {
	if err := c.Client.Validate(); err != nil {
		return err
	}
	if err := c.Grpc.Validate(); err != nil {
		return err
	}
	if err := c.Rest.Validate(); err != nil {
		return err
	}
	if err := c.Api.Validate(); err != nil {
		return err
	}
	if err := c.Authorization.Validate(); err != nil {
		return err
	}
	if err := c.Authentication.Validate(); err != nil {
		return err
	}
	if err := c.Store.Validate(); err != nil {
		return err
	}
	if err := c.Broker.Validate(); err != nil {
		return err
	}
	if err := c.Queue.Validate(); err != nil {
		return err
	}
	if err := c.Log.Validate(); err != nil {
		return err
	}
	if err := c.Routing.Validate(); err != nil {
		return err
	}
	return nil
}
func GetAppConfig(paths ...string) *Config {
	once.Do(func() {
		appConfig = getConfigRecord(paths...)
	})
	return appConfig
}

func GetCopyAppConfig(paths ...string) *Config {
	return getConfigRecord(paths...)
}
