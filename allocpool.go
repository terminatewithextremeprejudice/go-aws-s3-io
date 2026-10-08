package s3io

import "sync"

var tab32 [32]int = [32]int{
	0, 9, 1, 10, 13, 21, 2, 29,
	11, 14, 16, 18, 22, 25, 3, 30,
	8, 12, 20, 28, 15, 17, 24, 7,
	19, 27, 23, 6, 26, 5, 4, 31,
}

var tab64 [64]int = [64]int{
	63, 0, 58, 1, 59, 47, 53, 2, 60, 39, 48, 27, 54, 33, 42, 3, 61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4, 62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21, 56, 45, 25, 31, 35, 16, 9, 12, 44, 24, 15, 8, 23, 7, 6, 5}

func lg2(value uint64) uint64 {
	// return uint64(math.Log2(float64(value)))
	value |= value >> 1
	value |= value >> 2
	value |= value >> 4
	value |= value >> 8
	value |= value >> 16
	value |= value >> 32
	return uint64(tab64[uint64(((value-(value>>1))*0x07EDD5E59A4E28C2))>>58]) // tab64[(value>>1*0x07C4ACDD)>>27])
}

// pool to allocate of memory pages from

const maxalloc uint64 = MAX_MULTIPART_SIZE << 1

type pagelist struct {
	cache [][]byte
	m     sync.Mutex
}

type Allocpool struct {
	pages []pagelist
}

// Alloc returns a byte slice of length n and it does not retain reference to the slice
// so it may be gc'd by the runtime
func (a *Allocpool) Alloc(n int) []byte {
	var x []byte

	if uint64(n) >= maxalloc {
		panic("pool alloc: size too big")
	}

	if n == 0 {
		return nil
	}

	p := &a.pages[lg2(uint64(n))]
	p.m.Lock()

	if l := len(p.cache) - 1; l >= 0 {
		// cache hit
		x = p.cache[l]
		p.cache = p.cache[:l]
	}

	p.m.Unlock()

	if cap(x) < n {
		// cache miss, or the x is found is too small
		x = make([]byte, n)
	}

	return x[:n]
}

// Free returns a slice to the pool allocator
func (a *Allocpool) Free(b []byte) {
	if cap(b) == 0 || uint64(cap(b)) >= maxalloc {
		return // out of range
	}

	p := &a.pages[lg2(uint64(cap(b)))]

	p.m.Lock()
	p.cache = append(p.cache, b)
	p.m.Unlock()
}
