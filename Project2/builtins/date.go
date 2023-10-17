// builtins/date.go

package builtins

import (
	"fmt"
	"io"
	"time"
)

// Date handles the "date" built-in command.
func Date(w io.Writer) error {
	now := time.Now()
	_, err := fmt.Fprintln(w, now.Format("Mon Jan 2 15:04:05 MST 2006"))
	return err
}
