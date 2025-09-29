// pkg/config/config.go
package config

import (
	"fmt"
	"log"
	"log/slog"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	App      AppConfig      `mapstructure:"app"`
	Logging  LoggingConfig  `mapstructure:"logging"`
}

type ServerConfig struct {
	HTTPPort string `mapstructure:"http_port"`
}

type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Name     string `mapstructure:"name"`
	SSLMode  string `mapstructure:"ssl_mode"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	GroupID string   `mapstructure:"group_id"`
}

type AppConfig struct {
	WorkerCount    int    `mapstructure:"worker_count"`
	Environment    string `mapstructure:"environment"`
	MaxRetries     int    `mapstructure:"max_retries"`
	RetryDelayMs   int    `mapstructure:"retry_delay_ms"`
	BatchSize      int    `mapstructure:"batch_size"`
	CommitInterval int    `mapstructure:"commit_interval"`
}

type LoggingConfig struct {
	Level    string `mapstructure:"level"`
	Format   string `mapstructure:"format"`
	FilePath string `mapstructure:"file_path"`
}

var (
	globalConfig *Config
)

func Load(path string) *Config {
	if globalConfig != nil {
		return globalConfig
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(path)

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	viper.AutomaticEnv()

	if err := viper.Unmarshal(&globalConfig); err != nil {
		log.Fatalf("Unable to decode config into struct: %v", err)
	}

	slog.Info("Config loaded successfully", slog.Any("file", viper.ConfigFileUsed()))

	return globalConfig
}

func (c *Config) GetDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Database.Host,
		c.Database.Port,
		c.Database.User,
		c.Database.Password,
		c.Database.Name,
		c.Database.SSLMode,
	)
}

func (c *Config) GetKafkaBrokers() []string {
	return c.Kafka.Brokers
}
