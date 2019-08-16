package resp

import (
	"bytes"
	"io"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BulkReader", func() {
	// var buf = new(bytes.Buffer)
	//
	// setup := func() *RequestWriter {
	// 	buf.Reset()
	// 	return NewRequestWriter(buf)
	// }

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

	It("ReadAll: should not return ErrUnexpectedEOF", func() {
		bioR := &bufioR{
			rd: bytes.NewReader([]byte("$95\r\n" + strings.Repeat("x", 95) + "\r\n")),
			buf: make([]byte, 10),
		}

		stream, err := bioR.StreamBulk()
		Expect(err).To(BeNil())
		Expect(stream.Len()).To(Equal(int64(95)))

		all, err := stream.ReadAll()
		Expect(err).To(BeNil())
		Expect(len(all)).To(Equal(95))
		Expect(all).To(Equal([]byte(strings.Repeat("x", 95))))

		p := make([]byte, 10)
		stream.Read(p)
		rest, err := stream.Read(p)
		Expect(err).To(Equal(io.EOF))
		Expect(rest).To(Equal(0))
		Expect(p[0:2]).NotTo(Equal([]byte("\r\n")))
	})

	It("Read: should no CRLF left behind", func() {
		bioR := &bufioR{
			rd: bytes.NewReader([]byte("$95\r\n" + strings.Repeat("x", 95) + "\r\n")),
			buf: make([]byte, 100),
		}

		stream, err := bioR.StreamBulk()
		Expect(err).To(BeNil())
		Expect(stream.Len()).To(Equal(int64(95)))

		p := make([]byte, 100)
		stream.Read(p)
		rest, err := stream.Read(p)
		Expect(err).To(Equal(io.EOF))
		Expect(rest).To(Equal(0))
		Expect(p[0:2]).NotTo(Equal([]byte("\r\n")))
	})
})
