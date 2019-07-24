package libdag

func Contains(src []string, target string) bool {
	if (src == nil || len(src) == 0) {
		return false
	}
	for _, a := range src {
		if a == target {
			return true
		}
	}
	return false
}
