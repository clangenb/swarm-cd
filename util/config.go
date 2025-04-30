package util

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type StackConfig struct {
	Repo                 string
	Branch               string
	ComposeFile          string   `mapstructure:"compose_file"`
	ValuesFile           string   `mapstructure:"values_file"`
	SopsFiles            []string `mapstructure:"sops_files"`
	SopsSecretsDiscovery bool     `mapstructure:"sops_secrets_discovery"`
}

type RepoConfig struct {
	Url          string
	Username     string
	Password     string
	PasswordFile string `mapstructure:"password_file"`
}

type Config struct {
	ReposPath            string                  `mapstructure:"repos_path"`
	UpdateInterval       int                     `mapstructure:"update_interval"`
	Concurrency          int                     `mapstructure:"concurrency"`
	AutoRotate           bool                    `mapstructure:"auto_rotate"`
	StackConfigs         map[string]*StackConfig `mapstructure:"stacks"`
	RepoConfigs          map[string]*RepoConfig  `mapstructure:"repos"`
	SopsSecretsDiscovery bool                    `mapstructure:"sops_secrets_discovery"`
	Address              string                  `mapstructure:"address"`
}

var Configs Config

func LoadConfigs() (err error) {
	configsPath := getConfigsPath()
	Logger.Info(fmt.Sprintf("[Configs] path: %s", configsPath))

	err = readConfig(configsPath)
	if err != nil {
		return fmt.Errorf("could not read configuration file: %w", err)
	}

	err = readRepoConfigs(configsPath)
	if err != nil {
		return fmt.Errorf("could not read repos file: %w", err)
	}

	err = readStackConfigs(configsPath)
	if err != nil {
		return fmt.Errorf("could not load stacks file: %w", err)
	}
	return
}

func getConfigsPath() string {
	if path := os.Getenv("CONFIGS_PATH"); path != "" {
		return path
	}
	Logger.Info("[Configs] using default path `.`")
	return "." // Default path
}

func readConfig(path string) (err error) {
	configViper := viper.New()
	configViper.SetConfigName("config")
	configViper.AddConfigPath(path)
	configViper.SetDefault("update_interval", 120)
	configViper.SetDefault("concurrency", 3)
	configViper.SetDefault("repos_path", "repos")
	configViper.SetDefault("auto_rotate", true)
	configViper.SetDefault("sops_secrets_discovery", false)
	configViper.SetDefault("address", "0.0.0.0:8080")
	err = configViper.ReadInConfig()
	if err != nil && !errors.As(err, &viper.ConfigFileNotFoundError{}) {
		return
	}
	return configViper.Unmarshal(&Configs)
}

func readRepoConfigs(path string) (err error) {
	reposViper := viper.New()
	reposViper.SetConfigName("repos")
	reposViper.AddConfigPath(path)
	err = reposViper.ReadInConfig()
	if err != nil {
		return
	}

	// Reset maps before unmarshalling to remove old keys**
	Configs.RepoConfigs = make(map[string]*RepoConfig)

	return reposViper.Unmarshal(&Configs.RepoConfigs)
}

func readStackConfigs(path string) (err error) {
	stacksViper := viper.New()
	stacksViper.SetConfigName("stacks")
	stacksViper.AddConfigPath(path)
	err = stacksViper.ReadInConfig()
	if err != nil {
		return
	}

	// Reset maps before unmarshalling to remove old keys**
	Configs.StackConfigs = make(map[string]*StackConfig)

	return stacksViper.Unmarshal(&Configs.StackConfigs)
}
