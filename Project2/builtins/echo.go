package builtins

import (
	"fmt"
	"io"
	"strings"
)

func Echo(w io.Writer, args ...string) error {
	message := strings.Join(args, " ")
	_, err := fmt.Fprintln(w, message)
	return err
}
