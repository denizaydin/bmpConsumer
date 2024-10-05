package tools

import (
	cfg "bmpConsumer/config"
	"fmt"
	"strconv"

	"net"
	"strings"

	"github.com/golang/glog"
)

// creates manegement IP address from BGPID using igpNetwork to details.MGMTNetworkwr returns the "" if no match.
func MgmtIPfromBGPID(asNumber uint32, bgpid string, nmap *cfg.NetworkMap) string {

	glog.Infof("tring to convert bgpid:%s and as number:%v", bgpid, asNumber)
	for baseAs, details := range *nmap {
		if asNumber == baseAs {
			glog.Infof("found mapping for AS: %v, IGP Network: %s, MGMT Network: %s\n", asNumber, details.IGPNetwork, details.MGMTNetwork)
			return converBGPID2MGMTIP(bgpid, details.IGPNetwork, details.MGMTNetwork)
		}
	}
	lastThreeDigits := asNumber % 1000
	glog.Infof("tring to convert bgpid:%s and as number:%v", bgpid, lastThreeDigits)

	if lastThreeDigits > 0 && lastThreeDigits <= 99 {
		for baseAs, details := range *nmap {
			asstr := strconv.FormatUint(uint64(asNumber), 10)
			baseASstr := strconv.FormatUint(uint64(baseAs), 10)
			if asstr[:len(asstr)-3] == baseASstr[:len(baseASstr)-3] {
				glog.Infof("found mapping for AS: %v, IGP Network: %s, MGMT Network: %s\n", asNumber, details.IGPNetwork, details.MGMTNetwork)
				return converBGPID2MGMTIP(bgpid, details.IGPNetwork, details.MGMTNetwork)
			}
		}
		glog.Info("not matched to any network")
		return ""
	}
	glog.Info("not a leaf")
	return ""
}

// GetBGPIDfromAS returns bgp from as number for leaf types
func GetBGPIDfromAS(asNumber uint32, nmap *cfg.NetworkMap) (string, error) {
	for baseAs, details := range *nmap {
		if asNumber == baseAs {
			glog.Infof("found mapping for AS: %v, IGP Network: %s, MGMT Network: %s\n", asNumber, details.IGPNetwork, details.MGMTNetwork)
			_, igpNet, err := net.ParseCIDR(details.IGPNetwork)
			if err != nil {
				return "", fmt.Errorf("invalid IGP network: %v", err)
			}
			return igpNet.IP.String(), nil
		}
	}
	lastThreeDigits := asNumber % 1000
	if lastThreeDigits > 0 && lastThreeDigits <= 99 {
		for baseAs, details := range *nmap {
			asstr := strconv.FormatUint(uint64(asNumber), 10)
			baseASstr := strconv.FormatUint(uint64(baseAs), 10)
			if asstr[:len(asstr)-3] == baseASstr[:len(baseASstr)-3] {
				glog.Infof("found mapping for AS: %v, IGP Network: %s, MGMT Network: %s\n", asNumber, details.IGPNetwork, details.MGMTNetwork)
				// Parse networks
				_, igpNet, err := net.ParseCIDR(details.IGPNetwork)
				if err != nil {
					return "", fmt.Errorf("invalid IGP network: %v", err)
				}
				igpNetParts := strings.Split(igpNet.IP.String(), ".")
				// Parse the provided igpNet
				lastTwoDigits := asNumber % 100
				bgpID := fmt.Sprintf("%s.%s.%s.%d", igpNetParts[0], igpNetParts[1], igpNetParts[2], lastTwoDigits)
				return bgpID, nil
			}
		}
		return "", fmt.Errorf("not matched to any networkf")
	}
	return "", fmt.Errorf("not a leaf:%v, last three digit:%v", asNumber, lastThreeDigits)
}
func converBGPID2MGMTIP(bgpid string, igpNetwork string, mgmtNetwork string) string {
	// Parse networks
	_, igpNet, err := net.ParseCIDR(igpNetwork)
	if err != nil {
		glog.Warningf("invalid IGP network: %v", err)
		return bgpid
	}
	_, mgmtNet, err := net.ParseCIDR(mgmtNetwork)
	if err != nil {
		glog.Warningf("invalid MGMT network: %v", err)
		return bgpid
	}
	// Parse the provided IP
	parsedIP := net.ParseIP(bgpid)
	if parsedIP == nil {
		glog.Warningf("invalid bgpID address: %s, not in ip format", bgpid)
		return bgpid
	}
	// Check if the bgpID belongs to the IGP network
	if !igpNet.Contains(parsedIP) {
		glog.Warningf("bgpID %s is not within IGP network %s", bgpid, igpNetwork)
		return bgpid
	}
	// Convert IGP bgpID to MGMT bgpID
	parsedIPParts := strings.Split(bgpid, ".")
	mgmtParts := strings.Split(mgmtNet.String(), ".")
	// Replace the first three octets with the management network’s octets
	parsedIPParts[0], parsedIPParts[1], parsedIPParts[2] = mgmtParts[0], mgmtParts[1], mgmtParts[2]
	// Return the new IP
	newip := strings.Join(parsedIPParts, ".")
	return newip
}
