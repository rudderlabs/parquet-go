package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// TestNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex.
func TestNullCountsFromColumnIndex(t *testing.T) {
	type Entry struct {
		X *int64 `parquet:"name=x, type=INT64"`
		Y *int64 `parquet:"name=y, type=INT64"`
		Z *int64 `parquet:"name=z, type=INT64, omitstats=true"`
		U int64  `parquet:"name=u, type=INT64"`
		V int64  `parquet:"name=v, type=INT64, omitstats=true"`
	}

	type Expect struct {
		IsSetNullCounts bool
		NullCounts      []int64
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 1)
	assert.NoError(t, err)

	entries := []Entry{
		{val(0), val(0), val(0), 1, 1},
		{nil, val(1), val(1), 2, 2},
		{nil, nil, nil, 3, 3},
	}
	for _, entry := range entries {
		assert.NoError(t, pw.Write(entry))
	}
	assert.NoError(t, pw.WriteStop())

	pf, err := buffer.NewBufferFile(buf.Bytes())
	assert.Nil(t, err)
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.Nil(t, err)

	assert.Nil(t, pr.ReadFooter())

	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	chunks := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 5, len(chunks))

	expects := []Expect{
		{true, []int64{2}},
		{true, []int64{1}},
		{false, nil},
		{true, []int64{0}},
		{false, nil},
	}
	for i, chunk := range chunks {
		colIdx, err := readColumnIndex(pr.PFile, *chunk.ColumnIndexOffset)
		assert.NoError(t, err)
		assert.Equal(t, expects[i].IsSetNullCounts, colIdx.IsSetNullCounts())
		assert.Equal(t, expects[i].NullCounts, colIdx.GetNullCounts())
	}
}

// TestAllNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex if a field contains null value only.
func TestAllNullCountsFromColumnIndex(t *testing.T) {
	type Entry struct {
		X *int64 `parquet:"name=x, type=INT64"`
		Y *int64 `parquet:"name=z, type=INT64"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 1)
	assert.NoError(t, err)

	entries := []Entry{
		{val(0), nil},
		{val(1), nil},
		{val(2), nil},
		{val(3), nil},
		{val(4), nil},
		{val(5), nil},
	}
	for _, entry := range entries {
		assert.NoError(t, pw.Write(entry))
	}
	assert.NoError(t, pw.WriteStop())

	pf, err := buffer.NewBufferFile(buf.Bytes())
	assert.Nil(t, err)
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.Nil(t, err)

	assert.Nil(t, pr.ReadFooter())

	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	columns := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 2, len(columns))

	colIdx, err := readColumnIndex(pr.PFile, *columns[0].ColumnIndexOffset)
	assert.NoError(t, err)
	assert.Equal(t, true, colIdx.IsSetNullCounts())
	assert.Equal(t, []int64{0}, colIdx.GetNullCounts())

	colIdx, err = readColumnIndex(pr.PFile, *columns[1].ColumnIndexOffset)
	assert.NoError(t, err)
	assert.Equal(t, true, colIdx.IsSetNullCounts())
	assert.Equal(t, []int64{6}, colIdx.GetNullCounts())
}

func readColumnIndex(pf source.ParquetFile, offset int64) (*parquet.ColumnIndex, error) {
	colIdx := parquet.NewColumnIndex()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	triftReader := source.ConvertToThriftReader(pf, offset)
	protocol := tpf.GetProtocol(triftReader)
	err := colIdx.Read(context.Background(), protocol)
	if err != nil {
		return nil, err
	}
	return colIdx, nil
}

func val(x int64) *int64 {
	y := x
	return &y
}

func TestZeroRows(t *testing.T) {
	type test struct {
		ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	}

	var err error
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	//defer fw.Close()

	// write
	pw, err := NewParquetWriter(fw, new(test), 1)
	assert.NoError(t, err)

	err = pw.WriteStop()
	assert.NoError(t, err)
	assert.NoError(t, fw.Close())

	// read
	pf, err := buffer.NewBufferFile(buf.Bytes())
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, new(test), 1)
	assert.NoError(t, err)

	assert.Equal(t, int64(0), pr.GetNumRows())
}

type test struct {
	ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// TestNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex.
func TestDoubleWriteStop(t *testing.T) {
	var err error
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	//defer fw.Close()

	// write
	pw, err := NewParquetWriter(fw, new(test), 1)
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		stu := test{
			ColA: fmt.Sprintf("cola_%d", i),
			ColB: fmt.Sprintf("colb_%d", i),
		}
		assert.NoError(t, pw.Write(stu))
	}

	err = pw.WriteStop()
	assert.NoError(t, err)

	err = pw.WriteStop()
	assert.NoError(t, err)

	assert.NoError(t, fw.Close())

	// read
	pf, err := buffer.NewBufferFile(buf.Bytes())
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, new(test), 1)
	assert.NoError(t, err)

	num := int(pr.GetNumRows())
	rows := make([]test, num)
	err = pr.Read(&rows)
	assert.NoError(t, err)

	pr.ReadStop()
}

var testWriteErr = errors.New("test error")

type invalidFile struct {
	source.ParquetFile
}

func (m *invalidFile) Write(data []byte) (n int, err error) {
	return 0, testWriteErr
}

func TestNewWriterWithInvaidFile(t *testing.T) {
	pw, err := NewParquetWriter(&invalidFile{}, new(test), 1)
	assert.Nil(t, pw)
	assert.ErrorIs(t, err, testWriteErr)
}

// TestColumnIndexMinMaxAndNullPages tests that ColumnIndex min/max values and null pages are correctly set
// for different data types with various null patterns.
func TestColumnIndexMinMaxAndNullPages(t *testing.T) {

	type Entry struct {
		Name   *string  `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
		Age    *int32   `parquet:"name=Age, type=INT32, repetitiontype=OPTIONAL"`
		Id     *int64   `parquet:"name=Id, type=INT64, repetitiontype=OPTIONAL"`
		Weight *float32 `parquet:"name=Weight, type=FLOAT, repetitiontype=OPTIONAL"`
		Sex    *bool    `parquet:"name=Sex, type=BOOLEAN, repetitiontype=OPTIONAL"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 4)
	// Reduced page size to 64 to test pages with all null values for columns
	pw.PageSize = 64
	assert.NoError(t, err)

	// 40 rows with all non-null values
	for i := 0; i < 40; i++ {
		entry := Entry{
			Name:   strPtr(fmt.Sprintf("index_%d", i)),
			Age:    int32Ptr(int32(20 + i%5)),
			Id:     int64Ptr(int64(i + 1)),
			Weight: float32Ptr(float32(50.0 + float32(i)*0.1)),
			Sex:    boolPtr(i%2 == 0),
		}
		err = pw.Write(entry)
		assert.NoError(t, err)
	}

	// 40 rows with all null values
	for i := 40; i < 80; i++ {
		entry := Entry{
			Name:   nil,
			Age:    nil,
			Id:     nil,
			Weight: nil,
			Sex:    nil,
		}
		err = pw.Write(entry)
		assert.NoError(t, err)
	}

	// 40 rows with mixed null/non-null values
	probability := 0.4
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	for i := 80; i < 120; i++ {
		entry := Entry{
			Name:   nil,
			Age:    nil,
			Id:     nil,
			Weight: nil,
			Sex:    nil,
		}
		if r.Float64() < probability {
			entry.Name = strPtr(fmt.Sprintf("index_%d", i))
			entry.Age = int32Ptr(int32(20 + i%5))
			entry.Id = int64Ptr(int64(i))
			entry.Weight = float32Ptr(float32(50.0 + float32(i)*0.1))
			entry.Sex = boolPtr(i%2 == 0)
		}
		err = pw.Write(entry)
		assert.NoError(t, err)
	}

	assert.NoError(t, pw.WriteStop())

	pf, err := buffer.NewBufferFile(buf.Bytes())
	assert.Nil(t, err)
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.NoError(t, err)

	assert.NoError(t, pr.ReadFooter())

	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	columns := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 5, len(columns))

	// Test each column's ColumnIndex
	for i, column := range columns {
		colIdx, err := readColumnIndex(pr.PFile, *column.ColumnIndexOffset)
		assert.NoError(t, err, "Failed to read ColumnIndex for column %d", i)

		// Verify that ColumnIndex structure is valid
		assert.NotNil(t, colIdx, "ColumnIndex should not be nil for column %d", i)
		assert.NotNil(t, colIdx.NullPages, "NullPages should not be nil for column %d", i)
		assert.NotNil(t, colIdx.MinValues, "MinValues should not be nil for column %d", i)
		assert.NotNil(t, colIdx.MaxValues, "MaxValues should not be nil for column %d", i)

		// Verify that all arrays have the same length
		nullPagesLen := len(colIdx.NullPages)
		minValuesLen := len(colIdx.MinValues)
		maxValuesLen := len(colIdx.MaxValues)
		assert.Equal(t, nullPagesLen, minValuesLen, "NullPages and MinValues should have same length for column %d", i)
		assert.Equal(t, nullPagesLen, maxValuesLen, "NullPages and MaxValues should have same length for column %d", i)

		// Verify that if NullCounts is set, it has the correct length
		if colIdx.IsSetNullCounts() {
			assert.Equal(t, nullPagesLen, len(colIdx.NullCounts), "NullCounts should have same length as other arrays for column %d", i)
		}

		// Verify null pages and min/max values according to Parquet spec
		for j, isNullPage := range colIdx.NullPages {
			if isNullPage {
				// For null pages (pages with ONLY null values), min and max values should be empty byte arrays
				assert.Equal(t, 0, len(colIdx.MinValues[j]), fmt.Sprintf("MinValues should be empty for null page %d in column %d", j, i))
				assert.Equal(t, 0, len(colIdx.MaxValues[j]), fmt.Sprintf("MaxValues should be empty for null page %d in column %d", j, i))
			} else {
				// For non-null pages (pages with at least some non-null values), min and max values should not be empty
				assert.Greater(t, len(colIdx.MinValues[j]), 0, fmt.Sprintf("MinValues should not be empty for non-null page %d in column %d", j, i))
				assert.Greater(t, len(colIdx.MaxValues[j]), 0, fmt.Sprintf("MaxValues should not be empty for non-null page %d in column %d", j, i))
			}
		}

		// Verify that NullCounts is properly set for all columns (all are optional)
		assert.True(t, colIdx.IsSetNullCounts(), fmt.Sprintf("NullCounts should be set for column %d", i))
		assert.NotNil(t, colIdx.NullCounts, fmt.Sprintf("NullCounts should not be nil for column %d", i))

		// Verify that null counts are reasonable
		for j, nullCount := range colIdx.NullCounts {
			assert.GreaterOrEqual(t, nullCount, int64(0), fmt.Sprintf("NullCount should not be negative for page %d in column %d", j, i))
		}

		// Verify that we have the expected patterns
		// All columns should have some null pages due to the data pattern (40 non-null + 40 all-null + 40 mixed)
		hasNullPages := false
		for _, isNullPage := range colIdx.NullPages {
			if isNullPage {
				hasNullPages = true
				break
			}
		}

		// all columns should have some null pages
		assert.True(t, hasNullPages, fmt.Sprintf("Column %d should have some null pages", i))
	}
}

// Helper functions for creating pointers to primitive types
func float32Ptr(v float32) *float32 { return &v }
func boolPtr(v bool) *bool          { return &v }
func strPtr(v string) *string       { return &v }
func int32Ptr(v int32) *int32       { return &v }
func int64Ptr(v int64) *int64       { return &v }

// TestDisableColumnIndex tests that when disableColumnIndex is true, no ColumnIndex and OffsetIndex are written
func TestDisableColumnIndex(t *testing.T) {
	type Entry struct {
		Name   *string  `parquet:"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
		Age    *int32   `parquet:"name=Age, type=INT32, repetitiontype=OPTIONAL"`
		Id     *int64   `parquet:"name=Id, type=INT64, repetitiontype=OPTIONAL"`
		Weight *float32 `parquet:"name=Weight, type=FLOAT, repetitiontype=OPTIONAL"`
		Sex    *bool    `parquet:"name=Sex, type=BOOLEAN, repetitiontype=OPTIONAL"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)

	// Create writer with disableColumnIndex = true
	pw, err := NewParquetWriter(fw, new(Entry), 1, WithDisableColumnIndex(true))
	assert.NoError(t, err)

	// Write some test data
	for i := 0; i < 10; i++ {
		entry := Entry{
			Name:   strPtr(fmt.Sprintf("name_%d", i)),
			Age:    int32Ptr(int32(20 + i)),
			Id:     int64Ptr(int64(i + 1)),
			Weight: float32Ptr(float32(50.0 + float32(i)*0.1)),
			Sex:    boolPtr(i%2 == 0),
		}
		err = pw.Write(entry)
		assert.NoError(t, err)
	}
	assert.NoError(t, pw.WriteStop())

	// Read the written parquet file
	pf, err := buffer.NewBufferFile(buf.Bytes())
	assert.Nil(t, err)
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.NoError(t, err)

	assert.NoError(t, pr.ReadFooter())

	// Validate that no ColumnIndex and OffsetIndex were written
	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	columns := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 5, len(columns))

	// Check that all columns have no ColumnIndex and OffsetIndex
	for i, column := range columns {
		assert.Nil(t, column.ColumnIndexOffset, fmt.Sprintf("Column %d should have no ColumnIndexOffset", i))
		assert.Nil(t, column.ColumnIndexLength, fmt.Sprintf("Column %d should have no ColumnIndexLength", i))
		assert.Nil(t, column.OffsetIndexOffset, fmt.Sprintf("Column %d should have no OffsetIndexOffset", i))
		assert.Nil(t, column.OffsetIndexLength, fmt.Sprintf("Column %d should have no OffsetIndexLength", i))
	}

	// Verify that the writer's internal ColumnIndexes and OffsetIndexes slices are empty
	assert.Equal(t, 0, len(pw.ColumnIndexes), "ColumnIndexes should be empty when disabled")
	assert.Equal(t, 0, len(pw.OffsetIndexes), "OffsetIndexes should be empty when disabled")
}
