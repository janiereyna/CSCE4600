package main

import (
	"container/heap"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	/* CLI args*/
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	/* Load and parse processes */
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	/* Scheduling */
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	/* process .csv file */
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

/* region Schedulers
  FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
  an output writer
  a title for the chart
  a slice of processes */
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	/* The variables below are used to calculate the waiting time, turnaround time, and completion time for each process */
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	/* This piece of code sorts the processes by arrival time */
	for i := range processes {
		/* Calculate the waiting time for each process */
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		/* This piece of code calculates the start time for each process */
		start := waitingTime + processes[i].ArrivalTime

		/* This piece of code calculates the turnaround time for each process*/
		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		/* This piece of code calculates the completion time for each process*/
		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		/* This piece of code calculates the service time for each process*/
		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		/* This piece of code adds the gantt chart for each process */
		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	/* This piece of code calculates the average waiting time for all processes */
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	/* This piece of code outputs the title, gantt chart, and schedule table */
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

/* SJFPrioritySchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
 an output writer
 a title for the chart
 a slice of processes */
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	/* The variables below are used to calculate the waiting time, turnaround time, and completion time for each process */
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	/* Sort the processes by arrival time */
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	/* Priority queue for ready processes based on burst duration and priority */
	readyQueue := make(PriorityQueue, 0)
	heap.Init(&readyQueue)

	/* Process counter to keep track of completed processes */
	processCounter := 0

	for processCounter < len(processes) {
		/* Add processes that have arrived and are ready to the priority queue */
		for i := processCounter; i < len(processes); i++ {
			if processes[i].ArrivalTime <= serviceTime {
				/* Priority for SJF-Priority is calculated as the inverse of burst duration */
				priority := int(1.0 / float64(processes[i].BurstDuration))
				heap.Push(&readyQueue, &PriorityProcess{Process: processes[i], Priority: priority})
				processCounter++
			} else {
				break
			}
		}

		/* Pop the process with the highest priority (shortest burst duration) from the ready queue */
		current := heap.Pop(&readyQueue).(*PriorityProcess)
		currentProcess := current.Process
		waitingTime = serviceTime - currentProcess.ArrivalTime
		totalWait += float64(waitingTime)

		/* Calculate the start time for the current process */
		start := waitingTime + currentProcess.ArrivalTime

		/* Calculate the turnaround time for the current process */
		turnaround := currentProcess.BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		/* Calculate the completion time for the current process */
		completion := currentProcess.BurstDuration + currentProcess.ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		/* Calculate the service time for the current process */
		serviceTime += currentProcess.BurstDuration

		/* Add the Gantt chart for the current process */
		gantt = append(gantt, TimeSlice{
			PID:   currentProcess.ProcessID,
			Start: start,
			Stop:  serviceTime,
		})

		/* Update the schedule table for the current process */
		schedule[processCounter-1] = []string{
			fmt.Sprint(currentProcess.ProcessID),
			fmt.Sprint(currentProcess.Priority),
			fmt.Sprint(currentProcess.BurstDuration),
			fmt.Sprint(currentProcess.ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	/* Calculate the average waiting time for all processes */
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	/* Output the title, Gantt chart, and schedule table */
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

/* PriorityProcess represents a process with a priority value for SJF scheduling */
type PriorityProcess struct {
	Process  Process
	Priority int
}

/* PriorityQueue is a min-heap of PriorityProcess */
type PriorityQueue []*PriorityProcess

/* Len returns the number of elements in the priority queue */
func (pq PriorityQueue) Len() int { return len(pq) }

/* Less compares PriorityProcesses by their Priority values */
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

/* Swap swaps two elements in the priority queue */
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

/* Push adds a PriorityProcess to the priority queue */
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*PriorityProcess)
	*pq = append(*pq, item)
}

/* Pop removes and returns the top element (with the highest priority) from the priority queue */
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

/* SJFSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
 an output writer
 a title for the chart
 a slice of processes */
func SJFSchedule(w io.Writer, title string, processes []Process) {
	/* The variables below are used to calculate the waiting time, turnaround time, and completion time for each process */
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	/* Sort the processes by arrival time */
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	/* Priority queue for ready processes based on burst duration */
	readyQueue := make(PriorityQueue, 0)
	heap.Init(&readyQueue)

	/* Process counter to keep track of completed processes */
	processCounter := 0

	for processCounter < len(processes) {
		/* Add processes that have arrived and are ready to the priority queue */
		for i := processCounter; i < len(processes); i++ {
			if processes[i].ArrivalTime <= serviceTime {
				heap.Push(&readyQueue, &PriorityProcess{Process: processes[i], Priority: int(processes[i].BurstDuration)})
				processCounter++
			} else {
				break
			}
		}

		// Pop the process with the shortest burst duration from the ready queue */
		current := heap.Pop(&readyQueue).(*PriorityProcess)
		currentProcess := current.Process
		waitingTime = serviceTime - currentProcess.ArrivalTime
		totalWait += float64(waitingTime)

		// Calculate the start time for the current process */
		start := waitingTime + currentProcess.ArrivalTime

		// Calculate the turnaround time for the current process */
		turnaround := currentProcess.BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		// Calculate the completion time for the current process */
		completion := currentProcess.BurstDuration + currentProcess.ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		// Calculate the service time for the current process */
		serviceTime += currentProcess.BurstDuration

		// Add the Gantt chart for the current process */
		gantt = append(gantt, TimeSlice{
			PID:   currentProcess.ProcessID,
			Start: start,
			Stop:  serviceTime,
		})

		// Update the schedule table for the current process */
		schedule[processCounter-1] = []string{
			fmt.Sprint(currentProcess.ProcessID),
			fmt.Sprint(currentProcess.Priority),
			fmt.Sprint(currentProcess.BurstDuration),
			fmt.Sprint(currentProcess.ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	// Calculate the average waiting time for all processes */
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	// Output the title, Gantt chart, and schedule table */
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

/* RRSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
 an output writer
 a title for the chart
 a slice of processes */
func RRSchedule(w io.Writer, title string, processes []Process) {
	/* Constants for Round Robin scheduling */
	const quantum = 1 // Set the time quantum to 1 time unit

	/* The variables below are used to calculate the waiting time, turnaround time, and completion time for each process */
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	/* Queue to hold processes that are ready to execute */
	queue := make([]Process, 0)

	/* Process counter to keep track of completed processes */
	processCounter := 0

	for len(queue) > 0 || processCounter < len(processes) {
		/* Add processes that have arrived to the queue */
		for processCounter < len(processes) && processes[processCounter].ArrivalTime <= serviceTime {
			queue = append(queue, processes[processCounter])
			processCounter++
		}

		if len(queue) > 0 {
			/* Pop the next process from the front of the queue */
			currentProcess := queue[0]
			queue = queue[1:]

			/* Determine the actual time slice for this process (limited by quantum) */
			timeSlice := min(quantum, currentProcess.BurstDuration)

			/* Calculate the start time for the current process */
			start := max(serviceTime, currentProcess.ArrivalTime)

			/* Calculate the turnaround time for the current process */
			turnaround := timeSlice + max(0, start-currentProcess.ArrivalTime)
			totalTurnaround += float64(turnaround)

			/* Calculate the completion time for the current process */
			completion := start + timeSlice
			lastCompletion = float64(completion)

			/* Calculate the waiting time for the current process */
			waitingTime := max(0, start-currentProcess.ArrivalTime)
			totalWait += float64(waitingTime)

			/* Calculate the remaining burst duration for the current process */
			remainingBurst := currentProcess.BurstDuration - timeSlice

			/* Update the schedule table for the current process */
			schedule[currentProcess.ProcessID-1] = []string{
				fmt.Sprint(currentProcess.ProcessID),
				fmt.Sprint(currentProcess.Priority),
				fmt.Sprint(timeSlice),
				fmt.Sprint(start),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			/* Add the Gantt chart for the current process */
			gantt = append(gantt, TimeSlice{
				PID:   currentProcess.ProcessID,
				Start: start,
				Stop:  completion,
			})

			/* If the process has remaining burst, re-add it to the queue */
			if remainingBurst > 0 {
				currentProcess.BurstDuration = remainingBurst
				queue = append(queue, currentProcess)
			}

			/* Update the service time */
			serviceTime = completion
		} else {
			/* If the queue is empty, increment service time */
			serviceTime++
		}
	}

	/* Calculate the average throughput */
	count := float64(len(processes))
	aveThroughput := count / lastCompletion

	/* Output the title, Gantt chart, and schedule table */
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, totalWait/count, totalTurnaround/count, aveThroughput)
}

/* Helper function to find the minimum of two integers */
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

/* Helper function to find the maximum of two integers */
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

/* endregion */

/* region Output helpers */

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

/* region Loading processes. */

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}