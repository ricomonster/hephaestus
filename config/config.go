package config

import (
	"errors"
	"os"

	"github.com/spf13/viper"

	"github.com/ricomonster/hephaestus/aws"
)

type (
	AppConfig struct {
		App string
		Env string
	}

	Config struct {
		App *AppConfig
		AWS *aws.Config
	}
)

func Load(file string) (*Config, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	viper.SetConfigFile(file)
	viper.AddConfigPath(cwd)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// viper.SetDefault("PORT", "42069")
	viper.SetDefault("APP_NAME", "Diablo")
	viper.SetDefault("APP_ENV", "local")
	viper.SetDefault("AWS_REGION", "ap-southeast-1")

	c := &Config{
		App: &AppConfig{
			App: viper.GetString("APP_NAME"),
			Env: viper.GetString("APP_ENV"),
		},
		AWS: &aws.Config{
			Profile: viper.GetString("AWS_PROFILE"),
			Region:  viper.GetString("AWS_REGION"),
		},
	}

	return c, nil
}
