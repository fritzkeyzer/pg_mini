package pg_mini

import (
	"fmt"
	"time"
)

func prettyCount(count int64) string {
	sizes := []string{"", "k", "M", "B"}
	divisor := float64(1e3)
	countF := float64(count)
	for _, unit := range sizes {
		if countF < divisor {
			return fmt.Sprintf("%.0f%s", countF, unit)
		}
		countF /= divisor
	}
	return fmt.Sprintf("%.0f%s", countF, "T")
}

func prettyFileSize(size int64) string {
	sizes := []string{"B", "kB", "MB", "GB"}
	divisor := float64(1e3)
	fileSize := float64(size)
	for _, unit := range sizes {
		if fileSize < divisor {
			return fmt.Sprintf("%.0f%s", fileSize, unit)
		}
		fileSize /= divisor
	}
	return fmt.Sprintf("%.0f%s", fileSize, "TB")
}

func prettyDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	if d < 15*time.Second {
		return d.Round(10 * time.Millisecond).String()
	}
	if d < 30*time.Second {
		return d.Round(100 * time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}
