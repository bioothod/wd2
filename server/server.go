package main

import (
	"encoding/json"
	"flag"
	//"github.com/goji/param"
	"github.com/golang/glog"
	//"github.com/zenazn/goji"
	"github.com/bioothod/wd2/dbfs"
	"github.com/bioothod/wd2/middleware/auth"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
	"golang.org/x/net/webdav"
	"io/ioutil"
	"log"
	"net/http"
	//"os"
	//"strings"
)

func webdav_log(r *http.Request, err error) {
	if err != nil {
		glog.Errorf("%s: %s: headers: %+v, error: %v", r.Method, r.URL.Path, r.Header, err)
	}
}

type dbfs_webdav struct {
	fs *dbfs.DbFS
	locks webdav.LockSystem
	prefix string
}

func (dbh *dbfs_webdav) ServeHTTPC(c web.C, w http.ResponseWriter, r *http.Request) {
	username := auth.GetAuthUsername(c)
	if username == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="wd2"`)
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

type Config struct {
	Addr			string				`json:"addr"`
	AuthParams		string				`json:"auth"`
	DbFSParams		string				`json:"dbfs"`
	Ebucket			dbfs.EbucketCtl			`json:"ebucket"`
}

func main() {
	cpath := flag.String("config", "", "config file")
	flag.Parse()

	if *cpath == "" {
		log.Fatalf("You must provide config file")
	}

	cdata, err := ioutil.ReadFile(*cpath)
	if err != nil {
		log.Fatalf("Could not read config file '%s': %v", *cpath, err)
	}

	var conf Config
	err = json.Unmarshal(cdata, &conf)
	if err != nil {
		log.Fatalf("Could not unmarshal config file '%s': %v", *cpath, err)
	}

	actl, err := auth.NewAuthCtl("mysql", conf.AuthParams)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	fs, err := dbfs.NewDbFS("mysql", conf.DbFSParams, &conf.Ebucket)
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
	mux.Use(actl.BasicAuth)

	mux.Handle(dbh.prefix + "/*", dbh)

	http.ListenAndServe(conf.Addr, mux)
}
