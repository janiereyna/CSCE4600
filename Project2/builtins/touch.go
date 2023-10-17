// builtins/touch.go

package builtins

import (
	"fmt"
	"os"
)

// Touch handles the "touch" built-in command.
func Touch(args ...string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: touch <file>")
	}
	file := args[0]
	_, err := os.Create(file)
	return err
}
