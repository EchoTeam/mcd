.PHONY: all get-deps compile clean test-unit test-ct check distclean
REBAR := $(shell which ./rebar || which rebar)

PROJECTNAME=mcd
PROJECTVERSION=1.1.0

INSTALLDIR=$(LIBDIR)/$(PROJECTNAME)-$(PROJECTVERSION)/ebin
LIBDIR=$(shell erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell)

all: get-deps compile

compile:
	$(REBAR) compile

get-deps:
	$(REBAR) get-deps

test-unit: all
	$(REBAR) eunit skip_deps=true

test-ct: all
	memcached -d
	$(REBAR) ct skip_deps=true

check: test-unit test-ct

clean:
		$(REBAR) clean
		rm -rf ./ebin
		rm -rf ./logs
		rm -f ./erl_crash.dump
		rm -rf ./.eunit
		rm -f ./test/*.beam

distclean: clean
	rm -rf ./deps

install-lib:
	mkdir -p $(INSTALLDIR)
	for f in ebin/*.beam; do install $$f $(INSTALLDIR); done

# Get rebar if it doesn't exist

REBAR_URL=https://cloud.github.com/downloads/basho/rebar/rebar

./rebar:
	erl -noinput -noshell -s inets -s ssl \
		-eval 'httpc:request(get, {"${REBAR_URL}", []}, [], [{stream, "${REBAR}"}])' \
		-s init stop
	chmod +x ${REBAR}
