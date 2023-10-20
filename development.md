# Running Redis tests

`run_tests.sh` downloads and runs tests in Redis source code.
This script is a wrapper of the `runtest` script in Redis source,
and makes use of `runtest`'s feature to run tests against an external server.

```sh
# In separate terminals:

$ cargo run

$ ./run_tests.sh
```

By default, the server is expected to be bound to port 6379.

Options are passed through to `runtest`:

```sh
./run_tests.sh \
    --port 6380 \ # target port 6380
    --tags -slow  # skip slow tests
```

# Running fuzzers

```sh
cargo install cargo-fuzz

cd zakros-redis/fuzz

# list fuzzing targets
cargo fuzz list

# run fuzzer
cargo +nightly fuzz resp
```
