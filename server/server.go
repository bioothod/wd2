package main

import (
	//"errors"
	"flag"
	//"github.com/goji/param"
	//"github.com/golang/glog"
	//"github.com/zenazn/goji"
	"github.com/bioothod/wd2/middleware/auth"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
	"golang.org/x/net/webdav"
	"log"
	"net/http"
	//"os"
	//"strings"
)

func webdav_log(r *http.Request, err error) {
	log.Printf("%s: %s: headers: %+v, error: %v\n", r.Method, r.URL.Path, r.Header, err)
}

func main() {
	dir := flag.String("dir", "", "directory to serve")
	addr := flag.String("addr", "", "address to listen")
	auth_params := flag.String("auth", "", "mysql auth database parameters:\n" +
		"	user@unix(/path/to/socket)/dbname?charset=utf8\n" +
		"	user:password@tcp(localhost:5555)/dbname?charset=utf8\n" +
		"	user:password@/dbname\n" +
		"	user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname")
	flag.Parse()

	if *dir == "" {
		log.Fatalf("You must provide directory to serve")
	}
	if *addr == "" {
		log.Fatalf("You must provide address to listen")
	}
	if *auth_params == "" {
		log.Fatalf("You must provide correct database parameters")
	}

	actl, err := auth.NewAuthCtl("mysql", *auth_params)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	wd := &webdav.Handler {
		FileSystem: webdav.Dir(*dir),
		LockSystem: webdav.NewMemLS(),
		Logger: webdav_log,
	}

	mux := web.New()
	mux.Use(middleware.EnvInit)
	mux.Use(middleware.SubRouter)
	mux.Use(middleware.Logger)
	mux.Use(actl.BasicAuth)

	mux.Handle("/webdav/*", wd)

	http.ListenAndServe(*addr, mux)
}
