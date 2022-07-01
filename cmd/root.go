package cmd

import (
	"errors"
	"github.com/spf13/cobra"
	"log"
	"os"
	"time"
)

var RootCmd = &cobra.Command{
	Use:   "influx-stress",
	Short: "Create artificial load on an InfluxDB instance",
	Long:  "",
}

func Execute() {
	interval := os.Getenv("STRESS_INTERVAL")

	if interval != "" {
		tickerInterval, err := time.ParseDuration(interval)
		if err != nil {
			log.Printf("interval parse err:%s\n", err.Error())
			os.Exit(1)
		}
		log.Printf("interval :%s\n", interval)
		ticker := time.NewTicker(tickerInterval)
		defer ticker.Stop()

		done := make(chan error)

		for {
			if err := runCmdOnce(tickerInterval); err != nil {
				log.Println(err)
			}
			log.Println("complete once stress test")
			select {
			case err := <-done:
				if err != nil {
					log.Println(err)
				}
				return
			case <-ticker.C:
				continue
			}
		}
	}

	//run cmd once
	if err := runCmd(); err != nil {
		os.Exit(1)
	}
}

func runCmdOnce(tickerInterval time.Duration) error {
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	done := make(chan error)
	go func() {
		done <- runCmd()
	}()

	for {
		select {
		case err := <-done:
			return err
		case <-ticker.C:
			return errors.New("did not complete within its interval")
		}
	}
}

func runCmd() error {
	if err := RootCmd.Execute(); err != nil {
		return err
	}
	return nil
}
