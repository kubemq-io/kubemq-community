package grpc

import (
	"context"
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
	badCert = `-----BEGIN CERTIFICATE-----
MIIEbzCCAtegAwIBAgIRANAwRb/69uRYWBA7I/N1rZswDQYJKoZIhvcNAQELBQAw
gZExHjAcBgNVBAoTFW1rY2VydCBkZXZlbG9wbWVudCBDQTEzMDEGA1UECwwqREVT
S1RPUC1MTkI3UDIwXGxpb3IubmFiYXRAREVTS1RPUC1MTkI3UDIwMTowOAYDVQQD
DDFta2NlcnQgREVTS1RPUC1MTkI3UDIwXGxpb3IubmFiYXRAREVTS1RPUC1MTkI3
UDIwMB4XDTE5MDYwMTAwMDAwMFoXDTI5MTIyNDE1MTkzMlowazEnMCUGA1UEChMe
bWtjZXJ0IGRldmVsb3BtZW50IGNlcnRpZmljYXRlMUAwPgYDVQQLDDdERVNLVE9Q
LUxOQjdQMjBcbGlvci5uYWJhdEBERVNLVE9QLUxOQjdQMjAgKExpb3IgTmFiYXQp
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqHhzKoByHjE5O+dzc8Kw
/sni9L93LOkomtCK1kzJ5eQZwgBhqWPXPsrHLIks4SeMUMz6TBOPagYC22V5HKwC
yQsYH/BgMZNqNpNINRLnYV3HZXF/eQORVN+8sKC/OisnFI5ioi5JuM7urJRyoZcJ
LznGH52CEL8ioia6qjS2JSK2QyVSuCRbU+Yfn8+JalAWuuxn5RFEjYVVVWEOLec3
NEHM/yNkIiWPAJlq2wjK1rTXKF0C337xoV9bIpnUsXy3JQtHLWRUtLXfLhmIMKNA
yS4pkEnNnuWHJle+mR5czwlBsfgaOBvmMMacALOCV6gDyVr1hJPoN0fqBBhCKBv7
dQIDAQABo2cwZTAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEw
DAYDVR0TAQH/BAIwADAfBgNVHSMEGDAWgBTmAVGswAlUo2ofrfN6WGf7NB57/jAP
BgNVHREECDAGhwQAAAAAMA0GCSqGSIb3DQEBCwUAA4IBgQAEkd3XHL8fN9x7yRGQ
Skiq8HBq4KeePUa4SU0KZstc+KeubAvB7QlB5taDCmujoGydGcWkecoWP/VL15qu
vYOhGzSErA0RSm9q51nJ2lZevfkLuVL+v2xfuf+lr4Wbryrz2VJ+s7JZuUg06Opn
CcHCqERYmqoYvch/LtkTcG16Xhzq8VOpEo8VGsnkjNcTNMTOWHj3Bkcs03XBTcJ5
4ws62Ji17bgw1YwWH1zBiOPUlO7V7PVGvi9Syn7S6uSiDIRpVbP2FUg2yxzA794u
/K1YYnFDLtF/Ii/CMsieHD60ohRIbXF/qzlGY7Ow5T5XODVxD3UkKY5gPK1I8uSk
ilJFmhCuWID359b5EpaUOKh8C9pAin5g+4ZSVSQaMauUAdasZBjaPgpl0dHiAsUf
ZoCt7Tzu70dBR4tDBTHz0442jjQNxlynp65t6+P65zrG5kqTDNXCtQzkSh/qaBaB
QaWX/V+tLk3A9jvka69Z6sDuj06yUB/pyrEtEQc3kuiZg8w=
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

	testCa = `-----BEGIN CERTIFICATE-----
MIIE9DCCA1ygAwIBAgIRAMttD77JeToMiarbltEieXwwDQYJKoZIhvcNAQELBQAw
gZExHjAcBgNVBAoTFW1rY2VydCBkZXZlbG9wbWVudCBDQTEzMDEGA1UECwwqREVT
S1RPUC1MTkI3UDIwXGxpb3IubmFiYXRAREVTS1RPUC1MTkI3UDIwMTowOAYDVQQD
DDFta2NlcnQgREVTS1RPUC1MTkI3UDIwXGxpb3IubmFiYXRAREVTS1RPUC1MTkI3
UDIwMB4XDTE4MTEyNTE3Mjc0NloXDTI4MTEyNTE3Mjc0NlowgZExHjAcBgNVBAoT
FW1rY2VydCBkZXZlbG9wbWVudCBDQTEzMDEGA1UECwwqREVTS1RPUC1MTkI3UDIw
XGxpb3IubmFiYXRAREVTS1RPUC1MTkI3UDIwMTowOAYDVQQDDDFta2NlcnQgREVT
S1RPUC1MTkI3UDIwXGxpb3IubmFiYXRAREVTS1RPUC1MTkI3UDIwMIIBojANBgkq
hkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtmlegs3xrXi3Ccw4R281LP1C1Ra8IQBr
4OcxeHV5jEOgjDAdX9GuU4m1Bb8zmYeR0/VOh4S7CX/10IMRnc18q9cLm5wTUfPl
LG1Kr44jMdzjXq5lbJbqWD/AOfElBB2AMCALoQPUWYT523c0nxGJvjsVsL/+WHNQ
kohToKhX3NppkaHv7u5GJXaGHGrf/p6H+TRL8gu8QPSDoX7aGPuad3j4FQq/3IyS
vkZ1Or/ka8eTdPPGSjaWik7hOVBsKM4A/4nEGU7rIZDNGm9a88itsaWMh8O59oL9
XJUK18Ff+Ja+JC6PaOQvKwKpA+U1oEz76MYQwVcYZignlIpEH8qAYT/QoNs+J/pZ
8iL9wG6/LU5nQx88rbdoV72hQqphTyat0+hf6Uzgwy7RCRUkm0ElBrajom798mEV
ptui95LKczr/2tMpdCpPRE096gh2Y6QfsNv4lJY3DiFY7BrJ+zKKeKc7+zEztd3B
OsMhUaKbGNIO3JhG+RtnReQl5HLkPLjnAgMBAAGjRTBDMA4GA1UdDwEB/wQEAwIC
BDASBgNVHRMBAf8ECDAGAQH/AgEAMB0GA1UdDgQWBBTmAVGswAlUo2ofrfN6WGf7
NB57/jANBgkqhkiG9w0BAQsFAAOCAYEAJhJ1dmazjkUXZ5CMFT0qF+NYmNHs2NRc
WBDQa46VXuepu13IxAVdUSGLNUGK/H4WkKqS7bxiBUc/8lwrml6khI7j3c33EiDp
tKIlaUugbczmIfsRAypbA45ZwluVX8fnQNBVKbz9Mf0gdsLzPLd5ReOo6g4AzNHS
PsH2bb1o2hOG21d36stFtoUvcn4+Sa5/etFvZy++zu3ytsPdUjYBvpt9jRXa4+Tb
DkuKBk3MW6dBYDsyx9CtThjuhQT4Q7i7Ds1hjntbGGYZgpL1ssbhMjpGaFTmwbS1
RzisWhEtP8UJHv4SrrEDToWdd83aHpemp9j/NOr70r8H3BTW8tuaglqXStzvhX7x
w+1Xp35MNUF+gJCnbZ8UjoBG+207Oc1DK7AIpgAF/ro0iVXMzSrgDCQOPmlK9tax
obHstag0p3n8+TFVCUL5A1t6QBTsWiw/6PEwWnwT8+WrtBOl2J+I9wU3r6ZTct4H
Qs2jN+XOsEMkxIj/qshAGNUCy8TjjSM6
-----END CERTIFICATE-----`
)

func TestGRPCServer_TLS(t *testing.T) {
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
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC), sdk.WithCertificate(testCert, ""), sdk.WithCheckConnection(true))
	require.NoError(t, err)
	require.NotNil(t, client)
	err = client.Close()
	require.NoError(t, err)

}
func TestGRPCServer_BadTLS(t *testing.T) {
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
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC), sdk.WithCertificate(badCert, ""), sdk.WithCheckConnection(true))
	require.Error(t, err)
	require.Nil(t, client)
}

func TestGRPCServer_MTLS(t *testing.T) {
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
		Ca: &config.ResourceConfig{
			Filename: "",
			Data:     testCa,
		},
	}

	appConfig.Security = secConfig
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC), sdk.WithCertificate(testCert, ""), sdk.WithCheckConnection(true))
	require.NoError(t, err)
	require.NotNil(t, client)
	err = client.Close()
	require.NoError(t, err)

}
