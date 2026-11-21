package s3fs

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// credentials holds a static (or environment-derived) set of AWS credentials.
type credentials struct {
	accessKey    string
	secretKey    string
	sessionToken string
}

// resolveCredentials returns credentials from cfg, the environment, or the
// shared AWS credentials file, in that order.
func resolveCredentials(cfg *Config) (credentials, error) {
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		if cfg.AccessKey == "" || cfg.SecretKey == "" {
			return credentials{}, errors.New("s3fs: both AccessKey and SecretKey must be set")
		}
		return credentials{
			accessKey:    cfg.AccessKey,
			secretKey:    cfg.SecretKey,
			sessionToken: cfg.SessionToken,
		}, nil
	}

	if ak, sk := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"); ak != "" && sk != "" {
		return credentials{
			accessKey:    ak,
			secretKey:    sk,
			sessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		}, nil
	}

	if c, ok := sharedFileCredentials(); ok {
		return c, nil
	}

	return credentials{}, errors.New("s3fs: no credentials found: set AccessKey/SecretKey, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or ~/.aws/credentials")
}

// resolveRegion returns the configured region, falling back to the environment
// and the shared config file, and finally to us-east-1.
func resolveRegion(cfg *Config) string {
	if cfg.Region != "" {
		return cfg.Region
	}
	if r := os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
		return r
	}
	if r := sharedFileRegion(); r != "" {
		return r
	}
	return "us-east-1"
}

func awsProfile() string {
	if p := os.Getenv("AWS_PROFILE"); p != "" {
		return p
	}
	return "default"
}

func sharedCredentialsPath() string {
	if p := os.Getenv("AWS_SHARED_CREDENTIALS_FILE"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".aws", "credentials")
}

func sharedConfigPath() string {
	if p := os.Getenv("AWS_CONFIG_FILE"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".aws", "config")
}

func sharedFileCredentials() (credentials, bool) {
	values := readINISection(sharedCredentialsPath(), awsProfile())
	if values["aws_access_key_id"] == "" || values["aws_secret_access_key"] == "" {
		return credentials{}, false
	}
	return credentials{
		accessKey:    values["aws_access_key_id"],
		secretKey:    values["aws_secret_access_key"],
		sessionToken: values["aws_session_token"],
	}, true
}

func sharedFileRegion() string {
	profile := awsProfile()
	// The config file uses "[profile name]" sections while the credentials
	// file uses "[name]".
	if r := readINISection(sharedConfigPath(), "profile "+profile)["region"]; r != "" {
		return r
	}
	return readINISection(sharedConfigPath(), profile)["region"]
}

// readINISection returns the key/value pairs of the named section of an INI
// file. Missing files or sections yield an empty map.
func readINISection(path, section string) map[string]string {
	values := make(map[string]string)
	if path == "" {
		return values
	}
	f, err := os.Open(path)
	if err != nil {
		return values
	}
	defer f.Close()

	inSection := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inSection = strings.TrimSpace(line[1:len(line)-1]) == section
			continue
		}
		if !inSection {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		values[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return values
}
