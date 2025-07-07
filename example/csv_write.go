package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	var err error
	md := []string{
		"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL",
		"name=Age, type=INT32, repetitiontype=OPTIONAL",
		"name=Id, type=INT64, repetitiontype=OPTIONAL",
		"name=Weight, type=FLOAT, repetitiontype=OPTIONAL",
		"name=Sex, type=BOOLEAN, repetitiontype=OPTIONAL",
	}

	//write
	fw, err := local.NewLocalFileWriter("example/output/csv.parquet")
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pw, err := writer.NewCSVWriter(md, fw, 8, writer.WithDisableColumnIndex(true))
	pw.RowGroupSize = 1 * 1024 * 1024
	if err != nil {
		log.Println("Can't create csv writer", err)
		return
	}

	num := 5000000
	for i := 0; i < num; i++ {
		/*data := []string{
			fmt.Sprintf("%s_%d", "Student Name", i),
			fmt.Sprintf("%d", 20+i%5),
			fmt.Sprintf("%d", i),
			fmt.Sprintf("%f", 50.0+float32(i)*0.1),
			fmt.Sprintf("%t", i%2 == 0),
		}
		rec := make([]*string, len(data))
		for j := 0; j < len(data); j++ {
			rec[j] = &data[j]
		}
		if err = pw.WriteString(rec); err != nil {
			log.Println("WriteString error", err)
		}*/

		data2 := []interface{}{
			"Student Name",
			int32(i),
			nil,
			float32(50.0 + float32(i)*0.1),
			i%2 == 0,
		}
		if err = pw.Write(data2); err != nil {
			log.Println("Write error", err)
		}

	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	log.Println("Write Finished")
	fw.Close()

}
