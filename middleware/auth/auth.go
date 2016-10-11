package auth

import (
	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"fmt"
	"github.com/golang/glog"
	"github.com/zenazn/goji/web"
	"net/http"
	"time"
)

const AuthUsernameString = "Username"

type AuthCtl struct {
	db		*sql.DB
}

func NewAuthCtl(dbtype, dbparams string) (*AuthCtl, error) {
	db, err := sql.Open(dbtype, dbparams)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %s, params: %s: %v", dbtype, dbparams, err)
	}

	ctl := &AuthCtl {
		db:		db,
	}

	return ctl, nil
}

func (ctl *AuthCtl) Close() {
	ctl.db.Close()
}

type Mailbox struct {
	Username		string		`json:"username"`
	Password		string		`json:"password"`
	Created			time.Time	`json:"-"`
}

func (mbox *Mailbox) String() string {
	return fmt.Sprintf("username: %s, created: '%s'", mbox.Username, mbox.Created.String())
}

func (ctl *AuthCtl) NewUser(mbox *Mailbox) error {
	mbox.Created = time.Now()

	_, err := ctl.db.Exec("INSERT INTO users SET username=?,password=?,created=?",
		mbox.Username, mbox.Password, mbox.Created)
	if err != nil {
		return fmt.Errorf("could not insert new user: %s: %v", mbox.String(), err)
	}

	return nil
}

func (ctl *AuthCtl) DeleteUser(mbox *Mailbox) error {
	_, err := ctl.db.Exec("DELETE FROM users WHERE username=?", mbox.Username)
	if err != nil {
		return fmt.Errorf("could not delete user: %s: %v", mbox.String(), err)
	}

	return nil
}

func (ctl *AuthCtl) GetUser(mbox *Mailbox) error {
	rows, err := ctl.db.Query("SELECT * FROM users WHERE username=?", mbox.Username)
	if err != nil {
		return fmt.Errorf("could not read userinfo for user: %s: %v", mbox.Username, err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, password string

		err = rows.Scan(&username, &password, &mbox.Created)
		if err != nil {
			return fmt.Errorf("database schema mismatch: %v", err)
		}

		if password != mbox.Password || username != mbox.Username {
			return fmt.Errorf("username or password mismatch");
		} else {
			return nil
		}
	}

	err = rows.Err()
	if err != nil {
		return fmt.Errorf("could not scan database: %v", err)
	}

	return fmt.Errorf("there is no user %s", mbox.Username)
}

func (ctl *AuthCtl) UpdateUser(mbox *Mailbox) error {
	_, err := ctl.db.Exec("UPDATE users SET password=? WHERE username=?", mbox.Password, mbox.Username)
	if err != nil {
		return fmt.Errorf("could not update user: %s: %v", mbox.String(), err)
	}

	return nil
}

func (ctl *AuthCtl) Ping() error {
	return ctl.db.Ping()
}

func (ctl *AuthCtl) BasicAuth(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			estr := fmt.Sprintf("basic auth '%s' has failed", r.Header.Get("Authorization"))
			glog.Errorf("%s", estr)
			pleaseAuth(w, estr)
			return
		}

		mbox := Mailbox {
			Username: username,
			Password: password,
		}

		err := ctl.GetUser(&mbox)
		if err != nil {
			estr := fmt.Sprintf("invalid user '%s': %v", mbox.Username, err)
			glog.Errorf("%s", estr)
			pleaseAuth(w, estr)
			return
		}

		if c.Env == nil {
			c.Env = make(map[interface{}]interface{})
		}
		c.Env[AuthUsernameString] = mbox.Username

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func pleaseAuth(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", `Basic realm="wd2"`)
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte(msg))
}

func GetAuthUsername(c web.C) string {
	if c.Env == nil {
		return ""
	}
	v, ok := c.Env[AuthUsernameString]
	if !ok {
		return ""
	}
	if username, ok := v.(string); ok {
		return username
	}
	return ""
}
