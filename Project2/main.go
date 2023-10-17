package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"strings"

	"github.com/jar0582/CSCE4600/Project2/builtins" // Change the import path to your actual builtins package
)

func main() {
    exit := make(chan struct{}, 2) // buffer this so there's no deadlock.
    runLoop(os.Stdin, os.Stdout, os.Stderr, exit)
}

func runLoop(r io.Reader, w, errW io.Writer, exit chan struct{}) {
    var (
        input    string
        err      error
        readLoop = bufio.NewReader(r)
    )
    for {
        select {
        case <-exit:
            _, _ = fmt.Fprintln(w, "exiting gracefully...")
            return
        default:
            if err := printPrompt(w); err != nil {
                _, _ = fmt.Fprintln(errW, err)
                continue
            }
            if input, err = readLoop.ReadString('\n'); err != nil {
                _, _ = fmt.Fprintln(errW, err)
                continue
            }
            if err = handleInput(w, input, exit); err != nil {
                _, _ = fmt.Fprintln(errW, err)
            }
        }
    }
}

func printPrompt(w io.Writer) error {
    // Get current user.
    // Don't prematurely memoize this because it might change due to `su`?
    u, err := user.Current()
    if err != nil {
        return err
    }
    // Get current working directory.
    wd, err := os.Getwd()
    if err != nil {
        return err
    }

    // /home/User [Username] $
    _, err = fmt.Fprintf(w, "%v [%v] $ ", wd, u.Username)

    return err
}

func handleInput(w io.Writer, input string, exit chan<- struct{}) error {
    // Remove trailing spaces.
    input = strings.TrimSpace(input)

    // Split the input separate the command name and the command arguments.
    args := strings.Split(input, " ")
    name, args := args[0], args[1:]

  //commands
    switch name {
    case "cd":
        return builtins.ChangeDirectory(args...)
    case "env":
        return builtins.EnvironmentVariables(w, args...)
    case "exit": // Add "exit" built-in
        exit <- struct{}{} // Send a signal to exit.
        return nil // Don't return an error.
    case "echo":
        return builtins.Echo(w, args...) // Add "echo" 
    case "pwd":
        return builtins.Pwd(w) // Add "pwd" 
    case "touch":
		return builtins.Touch(args...) // Add "touch" 
    case "date":
		return builtins.Date(w) // Add "date" 
    }

    return executeCommand(name, args...)
}

func executeCommand(name string, arg ...string) error {
    // Otherwise prep the command
    cmd := exec.Command(name, arg...)

    // Set the correct output device.
    cmd.Stderr = os.Stderr
    cmd.Stdout = os.Stdout

    // Execute the command and return the error.
    return cmd.Run()
}
