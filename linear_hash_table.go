package main

import (
	"bufio"
	"container/list"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

type LinearHt struct {
	i                int
	recordNum        int
	maxBucketRecords int
	buckets          []*Bucket
}

type Bucket struct {
	maxBucketRecords int
	records          *list.List
	nextBucket       *Bucket
}

type Record struct {
	Key   string
	Value interface{}
}

/* Generic hash function (a popular one from Bernstein).
 * I tested a few and this was the best. */
func hashFunc(key string) int {
	var hash uint = 5381
	for i := 0; i < len(key); i++ {
		hash = ((hash << 5) + hash) + uint(key[i])
	}
	return int(hash)
}

func newBucket(maxBucketRecords int) *Bucket {
	bucket := new(Bucket)
	bucket.records = list.New()
	bucket.maxBucketRecords = maxBucketRecords
	return bucket
}

func (b *Bucket) Insert(record interface{}) *Bucket {
	bucket := b
	for bucket.records.Len() >= bucket.maxBucketRecords {
		bucket = bucket.nextBucket
	}
	bucket.records.PushBack(record)
	if bucket.records.Len() >= bucket.maxBucketRecords {
		nBucket := newBucket(bucket.maxBucketRecords)
		bucket.nextBucket = nBucket
		return nBucket
	}
	return bucket
}

func (b *Bucket) Delete(key string) bool {
	var prev *Bucket
	bucket := b
	for bucket != nil {
		for e := bucket.records.Front(); e != nil; e = e.Next() {
			record := e.Value.(Record)
			if record.Key == key {
				bucket.records.Remove(e)
				// shrink the extented buckets if possible
				if bucket.records.Len() == 0 && prev != nil {
					prev.nextBucket = bucket.nextBucket
				} else if prev != nil && prev.records.Len()+bucket.records.Len() < int(float64(bucket.maxBucketRecords)*0.7) {
					for e := bucket.records.Front(); e != nil; e = e.Next() {
						prev.records.PushBack(e)
					}
					prev.nextBucket = bucket.nextBucket
				}
				return true
			}
		}
		prev = bucket
		bucket = bucket.nextBucket
	}
	return false
}

func (b *Bucket) Find(key string) interface{} {
	bucket := b
	for bucket != nil {
		for e := bucket.records.Front(); e != nil; e = e.Next() {
			record := e.Value.(Record)
			if record.Key == key {
				return record.Value
			}
		}
		bucket = bucket.nextBucket
	}
	return nil
}

func newLinearHt(bucketNum int) *LinearHt {
	lh := new(LinearHt)
	fmt.Println(math.Log2(float64(bucketNum)))
	lh.i = int(math.Pow(2, math.Ceil(math.Log2(float64(bucketNum)))))
	lh.recordNum = 0
	lh.buckets = make([]*Bucket, bucketNum)
	lh.maxBucketRecords = 2
	for i := 0; i < bucketNum; i++ {
		lh.buckets[i] = newBucket(2)
	}
	return lh
}

func (lh *LinearHt) Expand() {
	nBucket := newBucket(lh.maxBucketRecords)
	newIdx := len(lh.buckets)
	if lh.i == len(lh.buckets) {
		lh.i <<= 1
	}
	lh.buckets = append(lh.buckets, nBucket)
	moveIdx := newIdx - int(lh.i>>1)
	fmt.Println("moveidx:", moveIdx)
	fmt.Println("newIdx:", newIdx)

	nMoveBucket := newBucket(lh.maxBucketRecords)
	moveBucket := lh.buckets[moveIdx]
	// change the old move bucket with the new one
	lh.buckets[moveIdx] = nMoveBucket
	var key string
	for {
		for l := moveBucket.records.Front(); l != nil; l = l.Next() {
			key = l.Value.(Record).Key
			if hashFunc(key)%lh.i == newIdx {
				nBucket = nBucket.Insert(l.Value)
			} else {
				nMoveBucket = nMoveBucket.Insert(l.Value)
			}
		}
		if moveBucket.records.Len() < moveBucket.maxBucketRecords {
			break
		} else {
			moveBucket = moveBucket.nextBucket
		}
	}
	fmt.Println("expand complete")
}

func (lh *LinearHt) Find(key string) interface{} {
	bucketIdx := hashFunc(key) % lh.i
	if bucketIdx > len(lh.buckets)-1 {
		bucketIdx -= lh.i >> 1
	}
	return lh.buckets[bucketIdx].Find(key)
}

func (lh *LinearHt) Insert(record Record) {
	lh.recordNum++
	if int(math.Floor(float64(len(lh.buckets))*1.7)) < lh.recordNum {
		lh.Expand()
	}
	bucketIdx := hashFunc(record.Key) % lh.i
	if bucketIdx > len(lh.buckets)-1 {
		bucketIdx -= lh.i >> 1
	}
	fmt.Println(bucketIdx)
	lh.buckets[bucketIdx].Insert(record)
}

func (lh *LinearHt) Delete(key string) {
	bucketIdx := hashFunc(key) % lh.i
	if bucketIdx > len(lh.buckets) {
		bucketIdx -= int(math.Pow(2, float64(lh.i-1)))
	}
	if lh.buckets[bucketIdx].Delete(key) {
		lh.recordNum--
	}
}

func (lh *LinearHt) BucketNum() int {
	return len(lh.buckets)
}

func (lh *LinearHt) PrintBucketRecords(i int) {
	if i > len(lh.buckets)-1 {
		return
	}
	bucket := lh.buckets[i]
	for bucket != nil {
		for l := bucket.records.Front(); l != nil; l = l.Next() {
			fmt.Println(l.Value)
		}
		bucket = bucket.nextBucket
	}
}

func (lh *LinearHt) RecordNum() int {
	return lh.recordNum
}

func (lh *LinearHt) BucketRecords(i int) int {
	if i > len(lh.buckets)-1 {
		return -1
	}
	records := 0
	bucket := lh.buckets[i]
	for bucket != nil {
		records += bucket.records.Len()
		bucket = bucket.nextBucket
	}
	return records
}

func main() {
	lh := newLinearHt(2)
	rd := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("LinearHT> ")
		line, err := rd.ReadString('\n')
		if err != nil {
			fmt.Println("Goodbye")
			os.Exit(1)
		}
		strs := strings.Fields(line)
		switch strings.ToLower(strs[0]) {
		case "add":
			if len(strs) != 3 {
				fmt.Println("Sorry, invalid add command format. Usage: [Add key value]")
				continue
			}
			record := Record{strs[1], strs[2]}
			lh.Insert(record)
		case "find":
			if len(strs) != 2 {
				fmt.Println("Sorry, invalid find command format. Usage: [Find key]")
				continue
			}
			record := lh.Find(strs[1])
			if record == nil {
				fmt.Printf("%v not exited!\n", strs[1])
			} else {
				fmt.Println(record)
			}
		case "del":
			if len(strs) != 2 {
				fmt.Println("Sorry, invalid del command format. Usage: [Del key]")
				continue
			}
			lh.Delete(strs[1])
		case "records":
			fmt.Printf("record num is %d\n", lh.RecordNum())
		case "buckets":
			fmt.Printf("Bucket num is %d\n", lh.BucketNum())
		case "bucketrecords":
			if len(strs) != 2 {
				fmt.Println("Sorry, invalid bucketrecords format. Usage: [BucketRecord bucketIdx]")
				continue
			}
			idx, err := strconv.Atoi(strs[1])
			if err != nil {
				fmt.Println("Sorry, Bucket idx must be integer")
				continue
			}
			fmt.Printf("Bucket %d record num is %d\n", idx, lh.BucketRecords(idx))
		case "p":
			if len(strs) != 2 {
				fmt.Println("Sorry, invalid bucketrecords format. Usage: [BucketRecord bucketIdx]")
				continue
			}
			idx, err := strconv.Atoi(strs[1])
			if err != nil {
				fmt.Println("Sorry, Bucket idx must be integer")
				continue
			}
			lh.PrintBucketRecords(idx)
		case "help":
			fallthrough
		case "-h":
			fallthrough
		case "--help":
			fmt.Println("Command list: [Add element], [RecordNum], [BucketRecords bucketIndex],[exit]")
		case "exit":
			fmt.Println("Goodbye~")
			os.Exit(1)
		}
	}
}
