package config

import (
	"encoding/base64"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/ghodss/yaml"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var appConfig *Config
var once sync.Once
var configFile = pflag.String("config", "./config.yaml", "set config file name")

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

func initFlags() {
	pflag.Parse()
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
		return "yaml"
	}
	err = toml.Unmarshal(in, c)
	if err == nil {
		return "toml"
	}
	return ""
}

func decodeBase64(in string) string {
	// base64 string cannot contain space so this is indication of base64 string
	if !strings.Contains(in, " ") {
		sDec, err := base64.StdEncoding.DecodeString(in)
		if err != nil {
			log.Println(fmt.Sprintf("error decoding config file base64 string: %s ", err.Error()))
			return in
		}
		return string(sDec)
	}
	return in
}
func getConfigDataFromEnv() (string, error) {
	envConfigData, ok := os.LookupEnv("CONFIG")
	envConfigData = decodeBase64(envConfigData)
	if ok {
		fileExt := getConfigFormat([]byte(envConfigData))
		if fileExt == "" {
			return "", fmt.Errorf("invalid environment config format")
		}
		err := ioutil.WriteFile("./config."+fileExt, []byte(envConfigData), 0600)
		if err != nil {
			return "", fmt.Errorf("cannot save environment config file")
		}
		return "./config." + fileExt, nil
	}
	return "", fmt.Errorf("no config data from environment variable")
}

func getConfigDataFromLocalFile(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	fileExt := getConfigFormat(data)
	if fileExt == "" {
		return "", fmt.Errorf("invalid file format")
	}
	if strings.HasSuffix(filename, "."+fileExt) {
		return filename, nil
	}
	return filename + "." + fileExt, nil
}

func getConfigFile() (string, error) {
	if *configFile != "" {
		loadedConfigFile, err := getConfigDataFromLocalFile(*configFile)
		if err != nil {
			return "", err
		}
		return loadedConfigFile, nil
	} else {
		loadedConfigFile, err := getConfigDataFromEnv()
		if err != nil {
			return "", err
		}
		return loadedConfigFile, nil
	}
}

func getConfigRecord(paths ...string) *Config {
	appConfig := GetDefaultConfig()
	appConfig.startServerTime = time.Now().Unix()
	initFlags()
	for _, paths := range paths {
		viper.AddConfigPath(paths)
	}
	path, err := os.Executable()
	if err != nil {
		return nil
	}
	loadedConfigFile, err := getConfigFile()
	if err == nil {
		viper.SetConfigFile(filepath.Join(filepath.Dir(path), loadedConfigFile))
	} else {
		log.Println(fmt.Sprintf("error on load config file: %s", err.Error()))
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
	_ = ioutil.WriteFile("./config.yaml", d, 0600)
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
