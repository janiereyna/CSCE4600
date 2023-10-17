package builtins

import (
	"os"
)

// Exit exits the shell.
func Exit() {
    os.Exit(0)
}
