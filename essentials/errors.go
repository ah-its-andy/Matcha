package essentials

import "fmt"

func NotFoundError(content string) error {
	return fmt.Errorf("Parameter %s not found.", content)
}
