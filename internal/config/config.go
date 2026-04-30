package config

import (
	"os"
	"strings"
)

type Config struct {
	Port  string
	Peers []string
}

func Load() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8001"
	}

	peerStr := os.Getenv("PEERS")
	var peers []string
	if peerStr != "" {
		peers = strings.Split(peerStr, ",")
	}

	return &Config{
		Port:  port,
		Peers: peers,
	}
}