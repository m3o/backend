package handler

import (
	"context"
	"errors"
	"strings"

	"github.com/gofrs/uuid"
	pb "github.com/micro/micro/v3/proto/api"
	"github.com/micro/micro/v3/service/client"
	"github.com/micro/micro/v3/service/logger"
	filesproto "github.com/micro/services/files/proto"
)

type V1 struct{}

func (e *V1) ServeInOne(ctx context.Context, req *pb.Request, rsp *pb.Response) error {
	files := filesproto.NewFilesService("files", client.DefaultClient)

	if len(req.Get) == 0 || len(req.Get["project"].Values) == 0 {
		return errors.New("bad request")
	}
	project := req.Get["project"].Values[0]
	logger.Infof("Serving %v", project)

	resp, err := files.List(ctx, &filesproto.ListRequest{
		Project: project,
	})
	if err != nil {
		return err
	}
	if len(resp.Files) == 0 {
		return errors.New("not found")
	}
	logger.Infof("%v files found for %v, length %v, title %v", len(resp.Files), project, len(resp.Files[0].FileContents), resp.Files[0].Name)
	// ? huh
	rsp.Header = make(map[string]*pb.Pair)
	rsp.Header["Content-Type"] = &pb.Pair{
		Key:    "Content-Type",
		Values: []string{"text/html", "charset=UTF-8"},
	}
	rsp.Body = resp.Files[0].FileContents
	return nil
}

func (e *V1) Serve(ctx context.Context, req *pb.Request, rsp *pb.Response) error {
	files := filesproto.NewFilesService("files", client.DefaultClient)

	if len(req.Get) == 0 || len(req.Get["project"].Values) == 0 {
		return errors.New("bad request")
	}
	project := req.Get["project"].Values[0]
	logger.Infof("Serving %v", project)

	resp, err := files.List(ctx, &filesproto.ListRequest{
		Project: project,
	})
	if err != nil {
		return err
	}
	if len(resp.Files) == 0 {
		return errors.New("not found")
	}

	htmlFile := ""
	jsFile := ""
	cssFile := ""
	for _, file := range resp.Files {
		switch {
		case strings.Contains(file.Path, "main"):
			jsFile = file.FileContents
		case strings.Contains(file.Path, "index"):
			htmlFile = file.FileContents
		case strings.Contains(file.Path, "style"):
			cssFile = file.FileContents
		}
	}
	id, _ := uuid.NewV4()

	rendered := `<html>
	<head>
		<style>` +
		cssFile +
		`</style>
	</head>
	<body>
	<div id="` + id.String() + `">
	</div>
	<script src="https://embedscript.com/assets/micro.js"></script>
	<script src="https://cdnjs.cloudflare.com/ajax/libs/mustache.js/4.1.0/mustache.min.js"></script>
	<script id="template" type="x-tmpl-mustache">` +
		htmlFile + `
	</script>
	<script>
	function render() {
		var template = document.getElementById('template').innerHTML;
		if (!view) {
			template.innerHTML = "Variable 'view' not found";
			return
		}
		var rendered = Mustache.render(template, view);
		document.getElementById('` + id.String() + `').innerHTML = rendered;
	}

	document.addEventListener("DOMContentLoaded", function (event) {` +
		jsFile +
		`})</script>` +
		`</body>
</html>`

	rsp.Header = make(map[string]*pb.Pair)
	rsp.Header["Content-Type"] = &pb.Pair{
		Key:    "Content-Type",
		Values: []string{"text/html", "charset=UTF-8"},
	}
	rsp.Body = rendered
	return nil
}
