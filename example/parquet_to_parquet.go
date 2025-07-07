package main

import (
	"log"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	// Input parquet file path
	inputFile := "example/data/corruptedcolumnindex.parquet"

	// Check if input file exists
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		log.Fatalf("Input file does not exist: %s", inputFile)
	}

	// Open source parquet file for reading
	srcFile, err := local.NewLocalFileReader(inputFile)
	if err != nil {
		log.Fatal("Failed to open source file:", err)
	}
	defer srcFile.Close()

	// Create parquet reader without predefined schema to infer schema
	pr, err := reader.NewParquetReader(srcFile, nil, 4)
	if err != nil {
		log.Fatal("Failed to create parquet reader:", err)
	}
	defer pr.ReadStop()

	// Get the inferred schema
	schemaHandler := pr.SchemaHandler
	log.Printf("Inferred schema with %d elements", len(schemaHandler.SchemaElements))

	// Print schema information
	log.Println("Schema elements:")
	for i, element := range schemaHandler.SchemaElements {
		if i == 0 {
			log.Printf("  [%d] Root: %s (children: %d)", i, element.GetName(), element.GetNumChildren())
		} else {
			log.Printf("  [%d] %s: type=%s, repetition=%s",
				i, element.GetName(), element.GetType(), element.GetRepetitionType())
		}
	}

	// Update schema elements to use external names (original column names)
	log.Println("Updating schema to use original column names...")
	for i := 1; i < len(schemaHandler.SchemaElements); i++ {
		element := schemaHandler.SchemaElements[i]
		externalName := schemaHandler.Infos[i].ExName
		element.Name = externalName
		log.Printf("  Updated [%d] %s -> %s", i, schemaHandler.Infos[i].InName, externalName)
	}

	// Read all data from the source file
	numRows := int(pr.GetNumRows())
	log.Printf("Reading %d rows from source file", numRows)

	// Read data as interface{} slice
	data, err := pr.ReadByNumber(numRows)
	if err != nil {
		log.Fatal("Failed to read parquet file:", err)
	}

	log.Printf("Successfully read %d rows", len(data))

	// Create output file
	outputFile := "example/output/parquet_to_parquet_output.parquet"

	// Ensure output directory exists
	outputDir := "example/output"
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Fatal("Failed to create output directory:", err)
		}
	}

	// Create destination file for writing
	dstFile, err := local.NewLocalFileWriter(outputFile)
	if err != nil {
		log.Fatal("Failed to create destination file:", err)
	}
	defer dstFile.Close()

	// Create parquet writer using the original schema directly
	pw, err := writer.NewParquetWriter(dstFile, schemaHandler.SchemaElements, 8, writer.WithDisableColumnIndex(true))
	if err != nil {
		log.Fatal("Failed to create parquet writer:", err)
	}
	// Process and write each row
	log.Println("Writing data to output file...")
	writtenRows := 0

	for _, row := range data {
		// Write the row directly without conversion
		if err := pw.Write(row); err != nil {
			log.Printf("Failed to write row %d: %v", writtenRows, err)
			continue
		}
		writtenRows++
	}

	// Close the writer
	if err := pw.WriteStop(); err != nil {
		log.Fatal("Failed to close parquet writer:", err)
	}

	log.Printf("Successfully wrote %d rows to %s", writtenRows, outputFile)
	log.Println("Parquet to parquet conversion completed successfully!")
}
