.PHONY: tests
.ONESHELL: tests

tests:
	docker run --name redisgraph-rs-tests -d --rm -p 6379:6379 redislabs/redisgraph \
		&& cargo test
	docker stop redisgraph-rs-tests
