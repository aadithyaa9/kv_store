package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port   string
	Peers  []string
	VNodes int     
}

func Load() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8001"
	}

	var peers []string
	if peerStr := os.Getenv("PEERS"); peerStr != "" {
		peers = strings.Split(peerStr, ",")
	}

	vnodes := 150                          
	if v := os.Getenv("VNODES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			vnodes = n
		}
	}                                      

	return &Config{
		Port:   port,
		Peers:  peers,
		VNodes: vnodes,                    
	}
}