package interceptor

import "net/http"

type Context struct {
	Request     *http.Request
	UpstreamURL string
}

type RequestHandler func(*Context) (*http.Response, bool, error)
type ResponseHandler func(*Context, *http.Response) (*http.Response, error)

type Interceptor interface {
	OnRequest(*Context) (*http.Response, bool, error)
	OnResponse(*Context, *http.Response) (*http.Response, error)
}

type Base struct{}

func (Base) OnRequest(*Context) (*http.Response, bool, error) {
	return nil, false, nil
}

func (Base) OnResponse(_ *Context, response *http.Response) (*http.Response, error) {
	return response, nil
}

func RunRequest(chain []Interceptor, ctx *Context) (*http.Response, bool, error) {
	for _, item := range chain {
		response, handled, err := item.OnRequest(ctx)
		if err != nil || handled {
			return response, handled, err
		}
	}
	return nil, false, nil
}

func RunResponse(chain []Interceptor, ctx *Context, response *http.Response) (*http.Response, error) {
	var err error
	for _, item := range chain {
		response, err = item.OnResponse(ctx, response)
		if err != nil {
			return nil, err
		}
	}
	return response, nil
}
