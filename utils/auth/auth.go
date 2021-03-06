package main

import (
	"flag"
	"fmt"
	"github.com/bioothod/wd2/dbfs"
	"github.com/bioothod/wd2/middleware/auth"
	"log"
	"os"
)

func main() {
	auth_params := flag.String("auth", "", "mysql auth database parameters:\n" +
		"	user@unix(/path/to/socket)/dbname?charset=utf8\n" +
		"	user:password@tcp(localhost:5555)/dbname?charset=utf8\n" +
		"	user:password@/dbname\n" +
		"	user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname")
	dbfs_params := flag.String("dbfs", "", "mysql dbfs data parameters:\n" +
		"	user@unix(/path/to/socket)/dbname?charset=utf8\n" +
		"	user:password@tcp(localhost:5555)/dbname?charset=utf8\n" +
		"	user:password@/dbname\n" +
		"	user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname")
	new_user := flag.String("new", "", "create new user")
	update_user := flag.String("update", "", "update user")
	check_user := flag.String("check", "", "verify user/password")
	pwd := flag.String("password", "", "password")
	flag.Parse()

	if *new_user == "" && *update_user == "" && *check_user == "" {
		log.Fatalf("You must provide username to create new user or update existing")
	}
	if *new_user != "" && *dbfs_params == "" {
		log.Fatalf("You must provide dbfs parameters when creating new user")
	}
	if *pwd == "" {
		log.Fatalf("You must provide password for the user")
	}
	if *auth_params == "" {
		log.Fatalf("You must provide correct database parameters")
	}

	actl, err := auth.NewAuthCtl("mysql", *auth_params)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer actl.Close()

	if *new_user != "" {
		fs, err := dbfs.NewDbFSWithoutBucket("mysql", *dbfs_params)
		if err != nil {
			log.Fatalf("Failed to initialize dbfs database: %v", err)
		}
		defer fs.Close()

		mbox := auth.Mailbox {
			Username: *new_user,
			Password: *pwd,
		}

		err = actl.NewUser(&mbox)
		if err != nil {
			log.Fatalf("Failed to create new user '%s': %v", mbox.Username, err)
		}

		u := &dbfs.DbFSUser {
			Username: mbox.Username,
			FS: fs,
		}
		err = u.Mkdir("/", 0755 | os.ModeDir)
		if err != nil {
			actl.DeleteUser(&mbox)

			log.Fatalf("Failed to create / directory for new user '%s': %v", mbox.Username, err)
		}

		fmt.Printf("New user '%s' has been created\n", mbox.Username)
	}

	if *update_user != "" {
		mbox := auth.Mailbox {
			Username: *update_user,
			Password: *pwd,
		}

		err = actl.UpdateUser(&mbox)
		if err != nil {
			log.Fatalf("Failed to update user '%s': %v", mbox.Username, err)
		}

		fmt.Printf("User '%s' has been updated\n", mbox.Username)
	}

	if *check_user != "" {
		mbox := auth.Mailbox {
			Username: *check_user,
			Password: *pwd,
		}

		err = actl.GetUser(&mbox)
		if err != nil {
			log.Fatalf("Failed to verify user '%s': %v", mbox.Username, err)
		}

		fmt.Printf("User '%s' has been verified: username/password match\n", mbox.Username)
	}
}
