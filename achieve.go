package main

import (
	"fmt"
	"os"
	"log"
 "time"
 "sync"
 "strings"
 "bufio"
 "io"
 "math"
"io/ioutil"
"runtime"
)
func main() {

	args := os.Args[1:]
	if len(args) != 6 { // for format  LogExtractor.exe -f From Time -t To Time -i Log file directory location
		fmt.Println("Please give proper command line arguments")
		return
	}
	filess, err := ioutil.ReadDir(args[5])
	if err != nil {
		fmt.Println(args[5])
        log.Fatal(err)
    }

	log_files := []string{}

	for _, f := range filess {
		log_files = append(log_files,f.Name())
	}

	startTimeArg := args[1]
	finishTimeArg := args[3]
	searchTime := time.Now()
	startIndex := binarySearch(log_files, args[5], args[1], args[3])
	fmt.Println("Time taken to search start Log(start file index) - ", time.Since(searchTime))
	fmt.Println("Start from file-",startIndex+1)
	s := time.Now()
	if startIndex != -1 {
		target_files := log_files[startIndex:]

	for _, f := range target_files{
	file, err := os.Open(args[5]+f)
	
	if err != nil {
		fmt.Println("cannot able to read the file", err)
		return
	}
	defer file.Close() //close after checking err
	
	queryStartTime, err := time.Parse(time.RFC3339, startTimeArg)
	if err != nil {
		fmt.Println("Could not able to parse the start time", startTimeArg)
		return
	}

	queryFinishTime, err := time.Parse(time.RFC3339, finishTimeArg)
	if err != nil {
		fmt.Println("Could not able to parse the finish time", finishTimeArg)
		return
	}

	filestat, err := file.Stat()
	if err != nil {
		fmt.Println("Could not able to get the file stat")
		return
	}

	fileSize := filestat.Size()
	offset := fileSize - 1
	lastLineSize := 0

	for {
		b := make([]byte, 1)
		n, err := file.ReadAt(b, offset)
		if err != nil {
			fmt.Println("Error reading file ", err)
			break
		}
		char := string(b[0])
		if char == "\n" {
			break
		}
		offset--
		lastLineSize += n
	}

	lastLine := make([]byte, lastLineSize)
	_, err = file.ReadAt(lastLine, offset+1)

	if err != nil {
		fmt.Println("Could not able to read last line with offset", offset, "and lastline size", lastLineSize)
		return
	}
	logSlice := strings.SplitN(string(lastLine), ",", 2)
	logCreationTimeString := logSlice[0]

	lastLogCreationTime, err := time.Parse(time.RFC3339, logCreationTimeString)
	if err != nil {
		fmt.Println("can not able to parse time : ", err)
	}
	fileSize1 := filestat.Size()
	offset1 := fileSize1 - fileSize1
	firstLineSize := 0

	for {
		ab := make([]byte, 1)
		n, err := file.ReadAt(ab, offset1)
		if err != nil {
			fmt.Println("Error reading file ", err)
			break
		}
		char := string(ab[0])
		if char == "\n" {
			break
		}
		offset1++
		firstLineSize += n
	}

	firstLine := make([]byte, firstLineSize)
	_, err = file.ReadAt(firstLine, offset1+1)

	if err != nil {
		fmt.Println("Could not able to read last line with offset", offset1, "and lastline size", firstLineSize)
		return
	}

	logSlice1 := strings.SplitN(string(firstLine), ",", 2)
	logCreationTimeString1 := logSlice1[0]

	firstLogCreationTime, err := time.Parse(time.RFC3339, logCreationTimeString1)
	if err != nil {
		fmt.Println("can not able to parse time : ", err)
	}

	if (lastLogCreationTime.After(queryStartTime) && (lastLogCreationTime.Before(queryFinishTime) || firstLogCreationTime.Before(queryFinishTime))){
		Process(file, queryStartTime, queryFinishTime)
	}else{
		break
	}
	PrintMemUsage()
}
fmt.Println("\nTime taken - ", time.Since(s))
}else{
	fmt.Println("No logs present in this time Range")
}
}

func Process(f *os.File, start time.Time, end time.Time) error {

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 250*1024)
		return lines
	}}

	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}

	r := bufio.NewReader(f)

	var wg sync.WaitGroup

	for {
		buf := linesPool.Get().([]byte)

		n, err := r.Read(buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Print("\nFiles processed")
				break
			}
			if err == io.EOF {
				break
			}
			return err
		}

		nextUntillNewline, err := r.ReadBytes('\n')

		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}

		wg.Add(1)
		go func() {
			ProcessChunk(buf, &linesPool, &stringPool, start, end)
			wg.Done()
		}()
		wg.Wait()

	}
	return nil
}

func ProcessChunk(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool, start time.Time, end time.Time) {

	var wg2 sync.WaitGroup

	logs := stringPool.Get().(string)
	logs = string(chunk)

	linesPool.Put(chunk)

	logsSlice := strings.Split(logs, "\n")

	stringPool.Put(logs)

	chunkSize := 300
	n := len(logsSlice)
	noOfThread := n / chunkSize

	if n%chunkSize != 0 {
		noOfThread++
	}

	for i := 0; i < (noOfThread); i++ {

		wg2.Add(1)
		go func(s int, e int) {
			defer wg2.Done() //to avaoid deadlocks
			for i := s; i < e; i++ {
				text := logsSlice[i]
				if len(text) == 0 {
					continue
				}
				logSlice := strings.SplitN(text, ",", 2)
				logCreationTimeString := logSlice[0]

				logCreationTime, err := time.Parse(time.RFC3339, logCreationTimeString)
				if err != nil {
					fmt.Printf("\n Could not able to parse the time :%s for log : %v", logCreationTimeString, text)
					return
				}

				if logCreationTime.After(start) && logCreationTime.Before(end) {
					fmt.Println(text)
				}
			}
			

		}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(logsSlice)))))
	}

	wg2.Wait()
	logsSlice = nil
}

func binarySearch(logs []string, address string,startTimeArg string,finishTimeArg string) int {
	start := 0
	end := len(logs)-1
	rindex := -1
	for start<=end{
		median := (start + end) / 2
		// fmt.Println(address+ logs[median])
		file, err := os.Open(address+ logs[median])
	
	if err != nil {
		fmt.Println("cannot able to read the file", err)
		return 0
	}
	
	defer file.Close() 

	queryStartTime, err := time.Parse(time.RFC3339, startTimeArg)

	if err != nil {
		fmt.Println("Could not able to parse the start time", startTimeArg)
		return 0
	}

	queryFinishTime, err := time.Parse(time.RFC3339, finishTimeArg)
	if err != nil {
		fmt.Println("Could not able to parse the finish time", finishTimeArg)
		return 0
	}

	filestat, err := file.Stat()
	if err != nil {
		fmt.Println("Could not able to get the file stat")
		return 0
	}

	fileSize1 := filestat.Size()
	offset1 := fileSize1 - fileSize1
	firstLineSize := 0

	for {
		ab := make([]byte, 1)
		n, err := file.ReadAt(ab, offset1)
		if err != nil {
			fmt.Println("Error reading file ", err)
			break
		}
		char := string(ab[0])
		if char == "\n" {
			break
		}
		offset1++
		firstLineSize += n
	}

	firstLine := make([]byte, firstLineSize)
	_, err = file.ReadAt(firstLine, offset1+1)

	if err != nil {
		fmt.Println("Could not able to read last line with offset", offset1, "and lastline size", firstLineSize)
		return 0
	}

	logSlice1 := strings.SplitN(string(firstLine), ",", 2)
	logCreationTimeString1 := logSlice1[0]

	firstLogCreationTime, err := time.Parse(time.RFC3339, logCreationTimeString1)
	if err != nil {
		fmt.Println("can not able to parse time : ", err)
	}
	// fmt.Println("first time : ",firstLogCreationTime)

	fileSize := filestat.Size()
	offset := fileSize - 1
	lastLineSize := 0

	for {
		b := make([]byte, 1)
		n, err := file.ReadAt(b, offset)
		if err != nil {
			fmt.Println("Error reading file ", err)
			break
		}
		char := string(b[0])
		if char == "\n" {
			break
		}
		offset--
		lastLineSize += n
	}
	lastLine := make([]byte, lastLineSize)
	_, err = file.ReadAt(lastLine, offset+1)

	if err != nil {
		fmt.Println("Could not able to read last line with offset", offset, "and lastline size", lastLineSize)
		return 0
	}

	logSlice := strings.SplitN(string(lastLine), ",", 2)
	logCreationTimeString := logSlice[0]

	lastLogCreationTime, err := time.Parse(time.RFC3339, logCreationTimeString)
	if err != nil {
		fmt.Println("can not able to parse time : ", err)
	}
	// fmt.Println("last line : ", lastLogCreationTime)
	queryFinishTime = queryFinishTime
	if lastLogCreationTime.After(queryStartTime) && firstLogCreationTime.After(queryStartTime){
		
		rindex = start
		end = median -1
	}else if lastLogCreationTime.After(queryStartTime) && firstLogCreationTime.Before(queryStartTime){
		rindex = median
		return rindex
	}else{
		rindex = start+1
		start = median + 1;
	}
}
	return rindex
}
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("\nMemory Usage(TotalAlloc) = %v MiB", bToMb(m.TotalAlloc))
}

func bToMb(b uint64) uint64 {
return b / 1024 / 1024
}