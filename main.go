package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

type Task struct {
	FilePath  string
	OutputDir string
}

type Result struct {
	Items []CrossrefItem
}

type CrossrefItem struct {
	DOI             string   `json:"DOI"`
	Title           []string `json:"title"`
	ReferencesCount int      `json:"references-count"`
	Author          []struct {
		Given  string `json:"given"`
		Family string `json:"family"`
	} `json:"author"`
	// 添加更多你感兴趣的字段
}

func main() {
	inputDir := "input_gz_files"       // 存放 .gz 文件的目录
	outputDir := "output"              // 解压后的输出目录
	logDir := "log"                    // 日志目录
	maxWorkers := runtime.NumCPU() * 2 // 可以根据实际环境调整

	if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
		fmt.Printf("Failed to create log directory: %v \n", err)
		return
	}

	logFileName := filepath.Join(logDir, fmt.Sprintf("decompression_%v.log", time.Now().Format("20060102150405")))

	// 创建 output 目录
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		fmt.Printf("Failed to create output directory: %v\n", err)
		return
	}

	// 初始化 logger
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v \n", err)
		return
	}
	defer logFile.Close()
	logger := log.New(io.MultiWriter(os.Stdout, logFile), "", log.Ldate|log.Ltime|log.Lshortfile)

	startTime := time.Now()
	err = concurrentDecompressMultipleFiles(inputDir, outputDir, maxWorkers, logger)
	if err != nil {
		logger.Printf("Error during concurrent file decompression: %v\n", err)
		return
	}
	logger.Println("All files successfully decompressed and processed!")
	logger.Println("Time:", time.Since(startTime))
}

func concurrentDecompressMultipleFiles(inputDir, outputDir string, maxWorkers int, logger *log.Logger) error {
	files, err := findGzFiles(inputDir)
	if err != nil {
		logger.Printf("Failed to find GZ files: %v\n", err)
		return fmt.Errorf("failed to find GZ files: %w", err)
	}

	var errCount uint32

	// 初始化 ants pool
	pool, err := ants.NewPool(maxWorkers, ants.WithPreAlloc(true))
	if err != nil {
		return fmt.Errorf("failed to create ants pool: %w", err)
	}
	defer pool.Release() // 优雅关闭

	for _, file := range files {
		logger.Printf("Processing file: %s\n", file)
		task := Task{FilePath: file, OutputDir: outputDir}
		if err := pool.Submit(func() {
			if err := processGzFile(task, logger); err != nil {
				logger.Printf("Error processing file %s: %v\n", task.FilePath, err)
				atomic.AddUint32(&errCount, 1)
			}
		}); err != nil {
			logger.Printf("Failed to submit task for file %s: %v\n", file, err)
			atomic.AddUint32(&errCount, 1)
		}
	}

	if atomic.LoadUint32(&errCount) > 0 {
		return fmt.Errorf("encountered %d errors during decompression", atomic.LoadUint32(&errCount))
	}
	return nil
}

func findGzFiles(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".gz" {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func processGzFile(task Task, logger *log.Logger) error {
	filePath := task.FilePath
	// outputDir := task.OutputDir
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open gzip file: %w, filepath %s", err, filePath)
	}
	defer file.Close()

	gr, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w, filepath %s", err, filePath)
	}
	defer gr.Close()

	decoder := json.NewDecoder(gr)
	var counter int64 = 0

	for {
		var item Result
		if err := decoder.Decode(&item); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode json %v:  filepath %s", err, filePath)
		}

		processCrossrefItem(item, filePath, logger) // 处理 JSON 数据
		counter++
	}

	return nil
}

// 处理解析后的 item
func processCrossrefItem(item Result, filePath string, logger *log.Logger) {
	for _, item := range item.Items {
		logger.Printf("DOI: %s, Title: %v, ReferencesCount: %v  file: %s", item.DOI, item.Title, item.ReferencesCount, filePath)
	}
}
