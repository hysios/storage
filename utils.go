package storage

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type BucketURI string

var (
	UnknownURI = "http://127.0.0.1:9000/keyue/unknown.png"
)

func SetUnknownURI(uri string) {
	UnknownURI = uri
}

func (uri BucketURI) UnknownURI() string {
	return UnknownURI
}

func (uri BucketURI) String() string {
	if Empty(string(uri)) {
		return uri.UnknownURI()
	}

	u, err := url.Parse(string(uri))
	if err != nil {
		return string(uri)
	}

	host, ok := GetBucketHost(u.Scheme, u.Host)
	if !ok {
		return string(uri)
	}

	// if isPrivatehost(host) {
	// 	if !config.GetBool("cloudmode.private") {
	// 		utils.ExternalIP()
	// 	}
	// }

	switch u.Scheme {
	case "minio", "s3", "qiniu":
		return fmt.Sprintf("http://%s/%s%s", host, u.Host, u.Path)
	default:
		return string(uri)
	}
}

func isPrivatehost(host string) bool {
	_host, _, _ := net.SplitHostPort(host)
	if _host == "localhost" {
		return true
	}

	if ip := net.ParseIP(_host); ip == nil {
		return false
	} else {
		return IsPrivateIP(ip)
	}
}

func (uri BucketURI) MarshalJSON() ([]byte, error) {
	return json.Marshal(uri.String())
}

func Empty(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

func QuoteBytes(u string) []byte {
	return []byte(strconv.Quote(u))
}

var stores sync.Map

type storeHost struct {
	Bucket string
	Host   string
	store  Storage
}

func register(scheme string, store Storage, host string) {
	var sts = []storeHost{{
		Bucket: store.BucketName(),
		Host:   host,
		store:  store,
	}}

	if olds, load := stores.LoadOrStore(scheme, sts); load {
		if stss, ok := olds.([]storeHost); ok {
			stss = append(stss, sts...)
			stores.Store(scheme, stss)
		}
	}
}

func GetBucketHost(scheme string, bucket string) (string, bool) {
	sts, ok := stores.Load(scheme)
	if !ok {
		return "", false
	}
	if stss, ok := sts.([]storeHost); !ok {
		return "", false
	} else {
		for _, st := range stss {
			if st.Bucket == bucket {
				return st.Host, true
			}
		}
	}

	return "", false
}

var (
	privateIPBlocks   []*net.IPNet
	availableIPBlocks []*net.IPNet
)

func init() {
	for _, cidr := range []string{
		"127.0.0.0/8",    // IPv4 loopback
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"169.254.0.0/16", // RFC3927 link-local
		"::1/128",        // IPv6 loopback
		"fe80::/10",      // IPv6 link-local
		"fc00::/7",       // IPv6 unique local addr
	} {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Errorf("parse error on %q: %v", cidr, err))
		}
		privateIPBlocks = append(privateIPBlocks, block)
	}

	for _, cidr := range []string{
		// "127.0.0.0/8",    // IPv4 loopback
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		// "::1/128",        // IPv6 loopback
		"fe80::/10", // IPv6 link-local
		"fc00::/7",  // IPv6 unique local addr
	} {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Errorf("parse error on %q: %v", cidr, err))
		}
		availableIPBlocks = append(availableIPBlocks, block)
	}
}

func IsPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}
