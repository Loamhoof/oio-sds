// OpenIO SDS Go rawx
// Copyright (C) 2015-2019 OpenIO SAS
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public
// License as published by the Free Software Foundation; either
// version 3.0 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

const (
	HeaderNameOioReqId = "X-oio-req-id"
	HeaderLenOioReqId  = 63
	HeaderNameTransId  = "X-trans-id"
	HeaderNameError    = "X-Error"
)

func setErrorString(rep http.ResponseWriter, s string) {
	rep.Header().Set(HeaderNameError, s)
}

func setError(rep http.ResponseWriter, e error) {
	setErrorString(rep, e.Error())
}

type rawxService struct {
	ns       string
	url      string
	path     string
	id       string
	repo     repository
	compress bool
	notifier Notifier
}

type rawxRequest struct {
	rawx      *rawxService
	req       *http.Request
	rep       http.ResponseWriter
	reqid     string
	startTime time.Time

	chunkID string
	chunk   chunkInfo

	// for the reply's purpose
	status   int
	bytesIn  uint64
	bytesOut uint64
}

func (rr *rawxRequest) drain() error {
	if _, err := io.Copy(ioutil.Discard, rr.req.Body); err != nil {
		rr.req.Close = true
		return err
	} else {
		return nil
	}
}

func (rr *rawxRequest) replyCode(code int) {
	rr.status = code
	rr.rep.WriteHeader(rr.status)
}

func (rr *rawxRequest) replyError(err error) {
	if os.IsExist(err) {
		rr.replyCode(http.StatusConflict)
	} else if os.IsPermission(err) {
		rr.replyCode(http.StatusForbidden)
	} else if os.IsNotExist(err) {
		rr.replyCode(http.StatusNotFound)
	} else {
		// A strong error occured, we tend to close the connection
		// whatever the client has sent in the request, in terms of
		// connection management.
		rr.req.Close = true

		// Also, we debug what happened in the reply headers
		// TODO(jfs): This is a job for a distributed tracing framework
		if logExtremeVerbosity {
			rr.rep.Header().Set("X-Error", err.Error())
		}

		// Prepare the most adapted reply status.
		if err == os.ErrInvalid {
			rr.replyCode(http.StatusBadRequest)
		} else {
			switch err {
			case errInvalidChunkID, errMissingHeader, errInvalidHeader:
				rr.replyCode(http.StatusBadRequest)
			case errInvalidRange:
				rr.replyCode(http.StatusRequestedRangeNotSatisfiable)
			default:
				rr.replyCode(http.StatusInternalServerError)
			}
		}
	}
}

func _dslash(s string) bool {
	return len(s) > 1 && s[0] == '/' && s[1] == '/'
}

func (rawx *rawxService) ServeHTTP(rep http.ResponseWriter, req *http.Request) {
	rawxreq := rawxRequest{
		rawx:      rawx,
		req:       req,
		rep:       rep,
		reqid:     "",
		startTime: time.Now(),
	}

	// Extract some common headers
	rawxreq.reqid = req.Header.Get(HeaderNameOioReqId)
	if len(rawxreq.reqid) <= 0 {
		rawxreq.reqid = req.Header.Get(HeaderNameTransId)
	}
	if len(rawxreq.reqid) > 0 {
		if len(rawxreq.reqid) > HeaderLenOioReqId {
			rawxreq.reqid = rawxreq.reqid[0:HeaderLenOioReqId]
		}
		rep.Header().Set(HeaderNameOioReqId, rawxreq.reqid)
	} else {
		// patch the reqid for pretty access log
		rawxreq.reqid = "-"
	}

	if len(req.Host) > 0 && (req.Host != rawx.id && req.Host != rawx.url) {
		rawxreq.replyCode(http.StatusTeapot)
	} else {
		for _dslash(req.URL.Path) {
			req.URL.Path = req.URL.Path[1:]
		}
		switch req.URL.Path {
		case "/info":
			rawxreq.serveInfo(rep, req)
		case "/stat":
			rawxreq.serveStat(rep, req)
		default:
			rawxreq.serveChunk()
		}
	}
}
