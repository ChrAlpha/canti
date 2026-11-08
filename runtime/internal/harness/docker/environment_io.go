package docker

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
)

func marshal(v any) ([]byte, error) { return json.Marshal(v) }
func decodeResponse(v any) func(*http.Response) error {
	return func(r *http.Response) error { return json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(v) }
}
func (x *Executor) archiveFile(ctx context.Context, id, path string, limit int64) ([]byte, error) {
	var data []byte
	err := x.api(ctx, "cp", http.MethodGet, "/containers/"+id+"/archive?path="+url.QueryEscape(path), nil, func(r *http.Response) error {
		tr := tar.NewReader(io.LimitReader(r.Body, limit+64<<10))
		h, err := tr.Next()
		if err != nil {
			return err
		}
		if h.Typeflag != tar.TypeReg || h.Size < 0 || h.Size > limit {
			return errors.New("docker: invalid or oversized environment evidence")
		}
		data, err = io.ReadAll(io.LimitReader(tr, limit+1))
		return err
	})
	return data, err
}
