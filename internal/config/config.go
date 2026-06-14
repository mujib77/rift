package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source SourceConfig `yaml:"source"`
	Destinations []DestinationConfig `yaml:"destinations"`
	Queue QueueConfig `yaml:"queue"`
	Filter FilterConfig `yaml:"filter"`
}

type SourceConfig struct {
	Type string `yaml:"type"`
	URL string `yaml:"url"`
	Slot string `yaml:"slot"`
	Publication string `yaml:"publication"`
	Tables []string `yaml:"tables"`
}

type DestinationConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	URL string `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
}

type QueueConfig struct {
	Enabled bool `yaml:"enabled"`
	Path string `yaml:"path"`
	MaxSizeMB int `yaml:"max_size_mb"`
}

type FilterConfig struct {
	Script string `yaml:"script"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if c.Source.URL == "" {
		return fmt.Errorf("source.url is required")
	}
	if c.Source.Slot == "" {
		c.Source.Slot = "rift_slot"
	}
	if c.Source.Publication == "" {
		c.Source.Publication = "rift_pub"
	}
	if len(c.Destinations) == 0 {
		return fmt.Errorf("at least one destination is required")
	}
	if c.Queue.Path == "" {
		c.Queue.Path = "./rift-queue"
	}
	if c.Queue.MaxSizeMB == 0 {
		c.Queue.MaxSizeMB = 1000
	}
	return nil
}