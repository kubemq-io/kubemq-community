package rest

import (
	"context"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/config"
	sdk "github.com/kubemq-io/kubemq-go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	testCert = `-----BEGIN CERTIFICATE-----
MIIEbjCCAtagAwIBAgIQfYzCwf/wl8w4OtjN73CozzANBgkqhkiG9w0BAQsFADCB
kTEeMBwGA1UEChMVbWtjZXJ0IGRldmVsb3BtZW50IENBMTMwMQYDVQQLDCpERVNL
VE9QLUxOQjdQMjBcbGlvci5uYWJhdEBERVNLVE9QLUxOQjdQMjAxOjA4BgNVBAMM
MW1rY2VydCBERVNLVE9QLUxOQjdQMjBcbGlvci5uYWJhdEBERVNLVE9QLUxOQjdQ
MjAwHhcNMTkwNjAxMDAwMDAwWhcNMjkxMjI0MTUxNTEzWjBrMScwJQYDVQQKEx5t
a2NlcnQgZGV2ZWxvcG1lbnQgY2VydGlmaWNhdGUxQDA+BgNVBAsMN0RFU0tUT1At
TE5CN1AyMFxsaW9yLm5hYmF0QERFU0tUT1AtTE5CN1AyMCAoTGlvciBOYWJhdCkw
ggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDyV2ZlFE0Tt8lG8tvabfk4
W/Ch1EQm8jwGQnxENso5KqRgUvhGHfSpd7gXvdtTqMKJ1RT9PmgtM8fftrOgSRWR
ZnJo5u8RbF3uLkwTvQGxNcmRmEPMXC+bST1oDbMBLtqlMsJDebSzY4zuv9etE1T2
jA59yshOahL2KBck2ohg4SLhuBvTGGBANSGIulbmr9QtEGQ3qITGAIGZPFF8d/rP
w5amtwSyaCKyNF2WemrxVzTo93f6igbpJLKI4CI2sZqFvyd5nJ3efgIoeo1pG8Yo
2HtdM7VRFOLAnP/BhszXzvwBgDIKcJIAmtz+5sdYES7btNfTxaIlvKyMzy3xGNjT
AgMBAAGjZzBlMA4GA1UdDwEB/wQEAwIFoDATBgNVHSUEDDAKBggrBgEFBQcDATAM
BgNVHRMBAf8EAjAAMB8GA1UdIwQYMBaAFOYBUazACVSjah+t83pYZ/s0Hnv+MA8G
A1UdEQQIMAaHBAAAAAAwDQYJKoZIhvcNAQELBQADggGBAERGliXrCZ8Xjupnzvpe
oPJwM/cEFuoVt7ka3I9juh1/3pQjw1VhVDXiyKIX8LbdsalEdEQ3Nah1Y2CIOjUd
RaSogP2ReTd5QPkQivssxlQTb+5cBHUM8V9/Fm596FZ29O612yrAeAM2N26r7Jkm
CSsdvxl/ZZylsnknVGRIIHYpKJFOUI+nq4wFIVm7a/VZ8D/EOsJfraElO0x1+lDF
txdoKAYGUymoqwX8v6+rv3jKyD6rON0jaYeUSLzKrdCNe1M5aQMxx2mql+rIHPOF
ng5WLFLO0zyBjDFvAjeYN9ot4I7iMoexl7SNoVJgopGA/u7K/+f+LdB8xN/E4IX4
DHczThCtLoTOkdlsAs2mESiitHSP/nXf5AJxpQTMG4chuxm7I14Y2KAdOHdtx5JN
apnkNF8ieJUXlrrIqxLbd+ItBq3oP+z25P2p/lAr29Q8wydLFptXdnzmY/6PUbKP
JgrLMeanOvBWBtKKHWN6tJEPTZt9GcFrtBnNJWhRxa8LHQ==
-----END CERTIFICATE-----`
	testKey = `-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDyV2ZlFE0Tt8lG
8tvabfk4W/Ch1EQm8jwGQnxENso5KqRgUvhGHfSpd7gXvdtTqMKJ1RT9PmgtM8ff
trOgSRWRZnJo5u8RbF3uLkwTvQGxNcmRmEPMXC+bST1oDbMBLtqlMsJDebSzY4zu
v9etE1T2jA59yshOahL2KBck2ohg4SLhuBvTGGBANSGIulbmr9QtEGQ3qITGAIGZ
PFF8d/rPw5amtwSyaCKyNF2WemrxVzTo93f6igbpJLKI4CI2sZqFvyd5nJ3efgIo
eo1pG8Yo2HtdM7VRFOLAnP/BhszXzvwBgDIKcJIAmtz+5sdYES7btNfTxaIlvKyM
zy3xGNjTAgMBAAECggEBAKHezS9Q+xbjmNcCGuXwtRn3F2kQvqEBBiTsPdLWggbj
O753TQyQr76Oj/GTyC8+NwsXwBhTmgQvZR9CCwNSLczcECmPrzoFF0yjsf8xLTMw
CT5t5UNYhBgGOLULCXkN0c+scuPdJFz6bsV+cNJTalnwPTG6xEbURWwUZTkhmxyR
mAIt2isB1UvBvPUjNaNNf2VsfFSMT/RqW978xhNFu1RtSpTANdFqWs5pWRG0gwKC
TgXOamazhvATvmJYcoJfQR9TDlMyAQ55sWPdWt9BdF3sXEqkyS2zatwvf0XKwBgN
hvkwJyKm8vTKeVcc7KuOe7jrTKlruDvlelk4V2pJdFkCgYEA8xAxmngf1kfj04E2
p6F6TS5YH5L3KKHI9fTAksIC1yLOS02oQr8cMi6vgVzmQoVd0uYnhHLk6xpe+lcj
OGHvwB7knbhaKPJ/9DIsw37wE4mj8gwAyY9FDOHxPUSv00excoWTeMSX3PLzCXgO
mqP8ovlW30yo1WNcxJlfmkZfoP8CgYEA/z1e7GTKL2utQAYfH0yVLi45eKrvT8Y8
g8HcQvEOcwQD2CVrljn2wsV5Cdh0x1roWCIG3pTSNJCh5zNgW0j3SMH0QRLMK/5P
YPpyH9rJzw8WKQFIyzcpdD6B3gBPebCZDejqLYuvEjcCvucnr70b2tq9tNl5h3Sy
OOhigKyqdC0CgYEA53KDGUjLYBrCeVLv/T1JHRdFOIOUMC+mEXaGrPhrBfqRn6kJ
0Mz0B2DnI/KXG76s8bbQ6FETZD+PMygoVHcFedaw8PJrf9QyPRBOCbXk22XUJBaD
5Wo0YSkAssul9TSuZpOFMplY1j7NaDXXCi+e0H1G2IjBt7fOzTISk+/w/XcCgYAE
X6XXwSZhx6ORXEl+PM61mt8rPSqaoFf7HgBLOVw5BlGWi5WbXmTnE4EudQITRHCE
yhh6CezML8pGbu/wwIBUQ9aOoubSvinYDJKWDya0IJsNmtMHgGt6bXPGPRUfjbIh
teMFYsZeNokaglWAwmnOxz7G8Y8OjiZbqUe+0radBQKBgQC+hmsXE/cpxhvGxsH2
nw7LJq5nHecpltxIZfS5abf6GbXKILQnlm98QDva54jg9N4CtaavJMOw1AH6rlHS
fzGT65B+/yDTvssTNxMSK7djPVQ4i+oeiMGhHJjihStDQhzEI+0lisk3z60oVq73
CmRxliAhDXR5wgmEfBcPIRi+Mw==
-----END PRIVATE KEY-----`
)

func TestRestServer_TLS(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	appConfig := getAppConfig()
	secConfig := &config.SecurityConfig{
		Cert: &config.ResourceConfig{
			Filename: "",
			Data:     testCert,
		},
		Key: &config.ResourceConfig{
			Filename: "",
			Data:     testKey,
		},
		Ca: nil,
	}

	appConfig.Security = secConfig
	_, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("https://0.0.0.0:%d", appConfig.Rest.Port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	require.NotNil(t, client)
	err = client.Close()
	require.NoError(t, err)

}
