package resp

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Command", func() {
	It("context can be extracted", func() {
		cmd := NewCommand("test")
		ctx := &CommandContext{ ClientID: 12 }

		cmd.SetContext(NewContext(cmd.Context(), ctx))
		extracted, ok := FromContext(cmd.Context())

		Expect(ok).To(Equal(true))
		Expect(extracted.ClientID).To(Equal(ctx.ClientID))
	})
})
