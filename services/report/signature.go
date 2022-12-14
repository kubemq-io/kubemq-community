package report

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net"
	"os"
)

type Signature struct {
	Host        string
	Fingerprint string
	Env         string
}

func CreateSignature() (*Signature, error) {
	s := &Signature{}
	env := runAt()
	switch env {
	case "kubernetes":
		host, ns, sig, err := kubernetesSignature()
		if err != nil {
			return nil, fmt.Errorf("error getting kubernetes signature, %s", err.Error())
		}
		s.Host = host
		s.Fingerprint = hashString(fmt.Sprintf("%s-%s-%s", host, ns, sig))
		s.Env = fmt.Sprintf("kubernetes-%s", ns)
		return s, err
	case "docker":
		host, mac, err := dockerSignature()
		if err != nil {
			return nil, fmt.Errorf("error getting docker signature, %s", err.Error())
		}
		s.Host = host
		s.Fingerprint = hashString(fmt.Sprintf("%s-%s", host, mac))
		s.Env = "docker"
		return s, err
	default:
		host, mac, err := standaloneSignature()
		if err != nil {
			return nil, fmt.Errorf("error getting standalone signature, %s", err.Error())
		}
		s.Host = host
		s.Fingerprint = hashString(fmt.Sprintf("%s-%s", host, mac))
		s.Env = "standalone"
		return s, err
	}
}

func hashString(str string) string {
	// Create a new SHA256 hash
	hash := sha256.New()

	// Write the string to the hash
	hash.Write([]byte(str))

	// Get the resulting hash as a byte slice
	hashBytes := hash.Sum(nil)

	// Convert the hash bytes to a hexadecimal string
	hashString := hex.EncodeToString(hashBytes)

	// Return the first 32 characters of the hash string
	return hashString[:32]
}

func dockerSignature() (string, string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", "", err
	}
	mac, err := ioutil.ReadFile("/sys/class/net/eth0/address")
	if err != nil {
		return "", "", err
	}
	return hostname, string(mac), nil
}
func kubernetesSignature() (string, string, string, error) {
	// get namespace
	namespace, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", "", "", err
	}
	token, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return "", "", "", err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", "", "", err
	}

	return hostname, string(namespace), string(token), nil

}
func standaloneSignature() (string, string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", "", err
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return "", "", err
	}
	return hostname, fmt.Sprintf("%v", interfaces), nil
}
func runAt() string {
	if _, err := os.Stat("/var/run/secrets/kubernetes.io"); err == nil {
		return "kubernetes"
	}
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return "docker"
	}
	return "standalone"
}
