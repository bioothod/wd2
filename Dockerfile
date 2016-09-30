FROM bioothod/wd2:latest

#RUN	echo "deb http://repo.reverbrain.com/trusty/ current/amd64/" > /etc/apt/sources.list.d/reverbrain.list && \
#	echo "deb http://repo.reverbrain.com/trusty/ current/all/" >> /etc/apt/sources.list.d/reverbrain.list && \
#	apt-get install -y curl && \
#	curl http://repo.reverbrain.com/REVERBRAIN.GPG | apt-key add - && \
#	apt-get update && \
#	apt-get upgrade -y && \
#	apt-get install -y curl git elliptics-client elliptics-dev g++ make && \
#	cp -f /usr/share/zoneinfo/posix/W-SU /etc/localtime && \
#	echo Europe/Moscow > /etc/timezeone

#RUN	VERSION=go1.7.1 && \
#	curl -O https://storage.googleapis.com/golang/$VERSION.linux-amd64.tar.gz && \
#	tar -C /usr/local -xf $VERSION.linux-amd64.tar.gz && \
#	rm -f $VERSION.linux-amd64.tar.gz

RUN	git config --global user.email "zbr@ioremap.net" && \
	git config --global user.name "Evgeniy Polyakov" && \
	export PATH=$PATH:/usr/local/go/bin && \
	mkdir -p /root/go && \
	export GOPATH=/root/go && \
	rm -rf ${GOPATH}/src/github.com/bioothod/elliptics-go && \
	rm -rf ${GOPATH}/src/github.com/bioothod/ebucket-go && \
	rm -rf ${GOPATH}/src/github.com/bioothod/wd2 && \
	rm -rf ${GOPATH}/pkg/* && \

	go get github.com/bioothod/elliptics-go/elliptics && \
	cd /root/go/src/github.com/bioothod/elliptics-go/elliptics && \
	git checkout master && \
	git pull && \
	git branch -v && \
	go install && \
	echo "Go bindings have been updated" && \

	cd /root/go/src/github.com/bioothod/ && \
	git clone http://github.com/bioothod/ebucket-go && \
	cd /root/go/src/github.com/bioothod/ebucket-go && \
	git branch -v && \
	make && \
	echo "Ebucket bindings have been updated" && \

	go get github.com/go-sql-driver/mysql && \
	go get github.com/golang/glog && \
	go get github.com/zenazn/goji/web && \
	go get github.com/zenazn/goji/web/middleware && \
	go get golang.org/x/net/webdav && \

	cd /root/go/src/github.com/bioothod && \
	git clone http://github.com/bioothod/wd2 && \
	cd /root/go/src/github.com/bioothod/wd2 && \
	git branch -v && \
	go build -o webdav_server server/server.go && \
	go build -o auth_ctl utils/auth/auth.go && \
	echo "wd2 has been updated"

EXPOSE 9090 80 443 8021
