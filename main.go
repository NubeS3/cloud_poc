package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/gin-gonic/gin"
	"io"
	"math"
	"net/http"
	"os"
)

func main() {
	fmt.Println("Meow!! App Started")
	gin.SetMode(gin.ReleaseMode)

	r := gin.Default()

	client, err := hdfs.New("tri:9000")
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Set a lower memory limit for multipart forms (default is 32 MiB)
	//r.MaxMultipartMemory = 8 << 20  // 8 MiB
	r.POST("/upload", func(c *gin.Context) {
		// Multipart form
		form, _ := c.MultipartForm()
		files := form.File["upload"]

		for _, file := range files {

			// Upload the file to specific dst.
			//c.SaveUploadedFile(file, "./upload/" + file.Filename)

			_ = client.Mkdir("/"+"userRinRinCute", 0777)

			hadoopFile, err := client.Create("/userRinRinCute/" + file.Filename)
			if err != nil {
				if e, ok := err.(*os.PathError); ok && e.Err == os.ErrExist {
					fmt.Println(err)
					continue //handle thêm số here pls
				}
				panic(err)
			}

			clientFile, err := file.Open()
			if err != nil {
				panic(err)
			}

			//data, err := ioutil.ReadAll(clientFile)
			//if err != nil {
			//	panic(err)
			//}

			data := make([]byte, 32767)
			totalPart := int(math.Ceil(float64(file.Size) / float64(32767)))

			for i := 0; i < totalPart; i++ {
				byteRead, err := clientFile.Read(data)
				if err != nil {
					fmt.Println(">=read meow====")
					fmt.Println(err)
				}
				fmt.Printf("%d byte read on %d total\n", byteRead, file.Size)

				byteWritten, err := hadoopFile.Write(data[:byteRead])
				if err != nil {
					fmt.Println(">=write meow====")
					fmt.Println(err)
				}
				fmt.Printf("%d byte written on %d total\n", byteWritten, file.Size)
			}

			_ = clientFile.Close()
			_ = hadoopFile.Close()
		}
		c.String(http.StatusOK, fmt.Sprintf("%d files uploaded!", len(files)))
	})

	r.GET("/download/:filename", func(c *gin.Context) {
		fn := c.Param("filename")
		fmt.Printf("Download %s\n", fn)
		file, err := client.Open("/userRinRinCute/" + fn)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err})
		}
		c.Writer.WriteHeader(http.StatusOK)
		c.Header("Pragma", "public")
		c.Header("Expires", "0")
		c.Header("Content-Description", "File Transfer")
		c.Header("Cache-Control", "must-revalidate, post-check=0, pre-check=0")
		c.Header("Content-Type", "application/octet-stream")
		c.Header("Content-Transfer-Encoding", "binary")
		c.Header("Content-Disposition", "attachment; filename=\""+fn+"\"")
		byteSent, err := io.Copy(c.Writer, file)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%d bytes sent to client", byteSent)

		//c.Writer.Write(data)
		//c.File(file.Name()

	})

	r.GET("/test", func(c *gin.Context) {
		err := client.CreateEmptyFile("/test")
		if err != nil {
			panic(err)
		}
		//err = client.Mkdir("/test", 0777)
		//if err != nil {
		//	err = client.CreateEmptyFile("/test/kitty.txt")
		//	if err != nil {
		//		panic(err)
		//	}
		//}

		c.JSON(http.StatusOK, gin.H{"message": "hah"})
	})
	r.Run(":8080")
}
