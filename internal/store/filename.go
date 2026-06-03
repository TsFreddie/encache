package store

import (
	"regexp"
	"strings"
)

var reservedWindowsNames = map[string]struct{}{
	"CON":  {},
	"PRN":  {},
	"AUX":  {},
	"NUL":  {},
	"COM1": {},
	"COM2": {},
	"COM3": {},
	"COM4": {},
	"COM5": {},
	"COM6": {},
	"COM7": {},
	"COM8": {},
	"COM9": {},
	"LPT1": {},
	"LPT2": {},
	"LPT3": {},
	"LPT4": {},
	"LPT5": {},
	"LPT6": {},
	"LPT7": {},
	"LPT8": {},
	"LPT9": {},
}

var invalidFilenameChars = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1f]`)
var repeatedDots = regexp.MustCompile(`\.{2,}`)

func SanitizeFilename(name string) string {
	if name == "" {
		return "unnamed"
	}

	sanitized := invalidFilenameChars.ReplaceAllString(name, "_")
	sanitized = repeatedDots.ReplaceAllString(sanitized, ".")
	sanitized = strings.TrimSpace(sanitized)
	sanitized = strings.TrimRight(sanitized, ". ")
	if sanitized == "" {
		return "unnamed"
	}

	upper := strings.ToUpper(sanitized)
	if _, ok := reservedWindowsNames[upper]; ok {
		sanitized = "_" + sanitized
	}
	if len([]rune(sanitized)) > 200 {
		runes := []rune(sanitized)
		sanitized = string(runes[:200])
	}
	return sanitized
}
