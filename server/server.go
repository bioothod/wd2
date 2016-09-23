package main

import (
	//"errors"
	"flag"
	//"github.com/goji/param"
	//"github.com/golang/glog"
	//"github.com/zenazn/goji"
	"github.com/bioothod/wd2/dbfs"
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

type dbfs_webdav struct {
	fs *dbfs.DbFS
	locks webdav.LockSystem
	prefix string
}

func (dbh *dbfs_webdav) ServeHTTPC(c web.C, w http.ResponseWriter, r *http.Request) {
	username := auth.GetAuthUsername(c)
	if username == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="Gritter"`)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("invalid request: please authorize"))
		return
	}

	fs := &dbfs.DbFSUser {
		FS: dbh.fs,
		Username: username,
	}

	wdh := &webdav.Handler {
		Prefix: dbh.prefix,
		FileSystem: fs,
		LockSystem: dbh.locks,
		Logger: webdav_log,
	}

	wdh.ServeHTTP(w, r)
}

func main() {
	addr := flag.String("addr", "", "address to listen")
	auth_params := flag.String("auth", "", "mysql auth database parameters:\n" +
		"	user@unix(/path/to/socket)/dbname?charset=utf8\n" +
		"	user:password@tcp(localhost:5555)/dbname?charset=utf8\n" +
		"	user:password@/dbname\n" +
		"	user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname")
	dbfs_params := flag.String("dbfs", "", "mysql direntry database parameters:\n" +
		"	user@unix(/path/to/socket)/dbname?charset=utf8\n" +
		"	user:password@tcp(localhost:5555)/dbname?charset=utf8\n" +
		"	user:password@/dbname\n" +
		"	user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname")
	flag.Parse()

	if *addr == "" {
		log.Fatalf("You must provide address to listen")
	}
	if *auth_params == "" {
		log.Fatalf("You must provide correct database parameters")
	}
	if *dbfs_params == "" {
		log.Fatalf("You must provide correct database parameters")
	}

	actl, err := auth.NewAuthCtl("mysql", *auth_params)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	fs, err := dbfs.NewDbFS("mysql", *dbfs_params)
	if err != nil {
		log.Fatalf("Could not create database controller: %v\n", err)
	}

	dbh := &dbfs_webdav {
		prefix: "/webdav",
		locks: webdav.NewMemLS(),
		fs: fs,
	}

	mux := web.New()
	mux.Use(middleware.EnvInit)
	mux.Use(middleware.SubRouter)
	mux.Use(middleware.Logger)
	mux.Use(actl.BasicAuth)

	mux.Handle(dbh.prefix + "/*", dbh)

	http.ListenAndServe(*addr, mux)
}
