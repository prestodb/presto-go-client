module github.com/prestodb/presto-go-client/prestoauth/oauth2

go 1.26.1

require (
	github.com/prestodb/presto-go-client v0.0.0
	github.com/stretchr/testify v1.11.1
	golang.org/x/oauth2 v0.27.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/prestodb/presto-go-client => ../..
