package webui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gocraft/web"
	"github.com/gojek/work/webui/internal/assets"
)

func mustAsset(name string) []byte {
	b, err := assets.Asset(name)
	if err != nil {
		panic(err)
	}
	return b
}

func render(rw web.ResponseWriter, jsonable interface{}, err error) {
	if err != nil {
		renderError(rw, err)
		return
	}

	jsonData, err := json.MarshalIndent(jsonable, "", "\t")
	if err != nil {
		renderError(rw, err)
		return
	}
	rw.Write(jsonData)
}

func renderError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(500)
	fmt.Fprintf(rw, `{"error": "%s"}`, err.Error())
}

func parsePage(r *web.Request) (uint, error) {
	err := r.ParseForm()
	if err != nil {
		return 0, err
	}

	pageStr := r.Form.Get("page")
	if pageStr == "" {
		pageStr = "1"
	}

	page, err := strconv.ParseUint(pageStr, 10, 0)
	return uint(page), err
}
