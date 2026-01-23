package matching

import (
	"github.com/BelWue/flowpipeline/pb"
)

// ClassifyAttackType determines the attack type based on protocol and port
func ClassifyAttackType(flow *pb.EnrichedFlow) string {
	proto := flow.GetProto()
	dstPort := flow.GetDstPort()

	// Check destination port first (most common for attacks)
	switch proto {
	case 6: // TCP
		switch dstPort {
		case 22:
			return "ssh_bruteforce"
		case 23:
			return "telnet_attack"
		case 25, 587, 465:
			return "smtp_attack"
		case 80, 8080, 8000, 8888:
			return "http_attack"
		case 443, 8443:
			return "https_attack"
		case 445:
			return "smb_attack"
		case 3306:
			return "mysql_attack"
		case 1433:
			return "mssql_attack"
		case 3389:
			return "rdp_attack"
		case 5432:
			return "postgresql_attack"
		case 5800, 5900:
			return "vnc_attack"
		case 6379:
			return "redis_attack"
		case 21:
			return "ftp_attack"
		case 110, 995:
			return "pop3_attack"
		case 143, 993:
			return "imap_attack"
		default:
			return "tcp_scan"
		}
	case 17: // UDP
		switch dstPort {
		case 53:
			return "dns_attack"
		case 123:
			return "ntp_amplification"
		case 161, 162:
			return "snmp_attack"
		case 1900:
			return "ssdp_amplification"
		case 5353:
			return "mdns_attack"
		case 389:
			return "ldap_attack"
		default:
			return "udp_flood"
		}
	case 1: // ICMP
		return "icmp_flood"
	default:
		return "unknown"
	}
}

// GetAttackTypeTag returns a unique tag ID for each attack type
func GetAttackTypeTag(attackType string) uint32 {
	baseTag := uint32(65001)

	attackTags := map[string]uint32{
		"ssh_bruteforce":      baseTag + 1,
		"telnet_attack":       baseTag + 2,
		"smtp_attack":         baseTag + 3,
		"http_attack":         baseTag + 4,
		"https_attack":        baseTag + 5,
		"smb_attack":          baseTag + 6,
		"mysql_attack":        baseTag + 7,
		"mssql_attack":        baseTag + 8,
		"rdp_attack":          baseTag + 9,
		"postgresql_attack":   baseTag + 10,
		"vnc_attack":          baseTag + 11,
		"redis_attack":        baseTag + 12,
		"ftp_attack":          baseTag + 13,
		"pop3_attack":         baseTag + 14,
		"imap_attack":         baseTag + 15,
		"dns_attack":          baseTag + 16,
		"ntp_amplification":   baseTag + 17,
		"snmp_attack":         baseTag + 18,
		"ssdp_amplification":  baseTag + 19,
		"mdns_attack":         baseTag + 20,
		"ldap_attack":         baseTag + 21,
		"udp_flood":           baseTag + 22,
		"icmp_flood":          baseTag + 23,
		"tcp_scan":            baseTag + 24,
		"unknown":             baseTag + 25,
	}

	if tag, ok := attackTags[attackType]; ok {
		return tag
	}
	return baseTag // fallback to generic bad_ip tag
}
