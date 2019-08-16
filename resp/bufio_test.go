package resp

import (
	"bytes"
	"io"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BulkReader", func() {
	It("ReadAll: should report error on closing prematurely", func() {
		bioR := &bufioR{
			rd: bytes.NewBufferString(strings.Repeat("x", 50)),
			buf: make([]byte, 10),
		}

		r := newBulkReader(bioR, 100)
		r.Close()
		ret, err := r.ReadAll()

		Expect(err).To(Equal(io.EOF))
		Expect(len(ret)).To(Equal(0))
	})

	It("ReadAll: should report error on closing unexpectedly", func() {
		bioR := &bufioR{
			rd: bytes.NewBufferString(strings.Repeat("x", 50)),
			buf: make([]byte, 10),
		}

		r := newBulkReader(bioR, 100)
		ret, err := r.ReadAll()

		Expect(err).To(Equal(io.ErrUnexpectedEOF))
		Expect(len(ret)).To(Equal(50))
	})
})
